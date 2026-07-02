/*
 * Copyright 2013-2026, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nextflow.executor

import java.nio.file.Path

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Global
import nextflow.file.FileHelper
import nextflow.processor.TaskRun
import nextflow.script.params.CmdEvalParam
import nextflow.script.params.EnvOutParam
import nextflow.script.params.OutParam
import nextflow.util.Duration
/**
 * Reads the exit status from a task's {@code .exitcode} file in a way that tolerates
 * the file being momentarily missing or empty right after the job is reported terminated
 * by a remote API (e.g. Kubernetes, AWS/Google/Azure Batch), which can happen when the
 * task work directory is backed by an eventually-consistent or network filesystem.
 *
 * Optionally waits for a set of sibling files (e.g. {@code .command.env}) to also be
 * present and non-empty before confirming the task is complete, providing stronger
 * assurance that the work directory is fully visible before task finalization begins.
 *
 * This is meant to be polled repeatedly from {@code TaskHandler#checkIfCompleted()}: a
 * {@code null} result means "keep waiting and call again on the next poll cycle" rather
 * than blocking, since {@code checkIfCompleted()} runs on a single shared polling thread.
 */
@Slf4j
@CompileStatic
class ExitStatusAwaiter {

    private final long timeoutMillis

    private long missingSinceMillis

    private long emptySinceMillis

    /** Each entry tracks [missingSinceMillis, emptySinceMillis] for one required sibling. */
    private final Map<Path, long[]> siblingTimestamps

    ExitStatusAwaiter(Duration timeout, List<Path> requiredSiblings = []) {
        this.timeoutMillis = timeout.toMillis()
        this.siblingTimestamps = new LinkedHashMap<>()
        for( Path p : requiredSiblings )
            siblingTimestamps[p] = new long[2]
    }

    /**
     * Returns the list of sibling files that must be visible and non-empty alongside the
     * exit file before a task is declared complete. Currently that means {@code .command.env}
     * when the task declares {@code env} or {@code eval} outputs, since those are written
     * before {@code .exitcode} by the wrapper script but may arrive later on NFS/PVCs.
     */
    static List<Path> envSiblings(TaskRun task) {
        final outputs = task.outputs
        if( !outputs )
            return Collections.<Path>emptyList()
        for( OutParam param : outputs.keySet() )
            if( param instanceof EnvOutParam || param instanceof CmdEvalParam )
                return [task.workDir.resolve(TaskRun.CMD_ENV)] as List<Path>
        return Collections.<Path>emptyList()
    }

    /** Clears accumulated missing/empty timestamps so the next read() starts fresh. */
    void reset() {
        missingSinceMillis = 0
        emptySinceMillis = 0
        for( long[] ts : siblingTimestamps.values() ) {
            ts[0] = 0
            ts[1] = 0
        }
    }

    /**
     * @param exitFile The path to the task's {@code .exitcode} file
     * @return
     *      the exit status read from the file once it's present and has content (and all
     *      required siblings are also visible),
     *      {@code null} if any awaited file is missing or empty but still within the timeout
     *      window (the caller should treat the task as not-yet-complete and retry on the
     *      next poll cycle),
     *      or {@link Integer#MAX_VALUE} once the timeout has elapsed without finding
     *      usable content.
     */
    Integer read(Path exitFile) {
        final exitStatus = readExitFile(exitFile)
        if( exitStatus == null || exitStatus == Integer.MAX_VALUE )
            return exitStatus
        final result = checkSiblings(exitStatus)
        // Once the exit file and all sentinels are confirmed, flush the head-node's
        // NFS attribute cache for the work directory so the task finalizer reads
        // output files with up-to-date sizes rather than stale cached 0-byte views.
        if( result != null && result != Integer.MAX_VALUE && exitFile )
            refreshWorkDir(exitFile.parent)
        return result
    }

    private void refreshWorkDir(Path workDir) {
        if( Global.session && FileHelper.workDirIsSharedFS )
            FileHelper.listDirectory(workDir)
    }

    private Integer readExitFile(Path exitFile) {
        final attrs = exitFile ? FileHelper.readAttributes(exitFile) : null
        if( !attrs || !attrs.lastModifiedTime()?.toMillis() ) {
            if( !missingSinceMillis ) {
                log.trace "Exit file does not exist yet -- waiting: ${exitFile}"
                missingSinceMillis = System.currentTimeMillis()
                return null
            }
            final delta = System.currentTimeMillis() - missingSinceMillis
            if( delta < timeoutMillis )
                return null
            log.warn "Unable to find exit status file after ${delta}ms: ${exitFile?.toUriString()}"
            return Integer.MAX_VALUE
        }

        final status = exitFile.text?.trim()
        if( status ) {
            try {
                return status.toInteger()
            }
            catch( Exception e ) {
                log.warn "Unable to parse process exit file: ${exitFile.toUriString()} -- bad value: '$status'"
                return Integer.MAX_VALUE
            }
        }

        // file exists but it's empty -- this can happen because of write propagation
        // delays on shared/eventually-consistent filesystems; wait before giving up
        if( !emptySinceMillis ) {
            log.debug "Exit file is empty -- waiting: ${exitFile}"
            emptySinceMillis = System.currentTimeMillis()
            return null
        }
        final delta = System.currentTimeMillis() - emptySinceMillis
        if( delta < timeoutMillis )
            return null
        log.warn "Exit status file is still empty after ${delta}ms: ${exitFile.toUriString()}"
        return Integer.MAX_VALUE
    }

    private Integer checkSiblings(Integer exitStatus) {
        for( Map.Entry<Path, long[]> entry : siblingTimestamps.entrySet() ) {
            final ready = checkSibling(entry.key, entry.value)
            if( ready == null ) return Integer.MAX_VALUE   // timed out waiting for sibling
            if( !ready ) return null                        // sibling not yet visible; defer
        }
        return exitStatus
    }

    /**
     * @return {@code Boolean.TRUE} if the sibling is visible and non-empty,
     *         {@code Boolean.FALSE} if it's absent/empty but within the timeout window,
     *         {@code null} if the timeout has elapsed without finding usable content.
     */
    private Boolean checkSibling(Path path, long[] ts) {
        final attrs = FileHelper.readAttributes(path)
        if( !attrs || !attrs.lastModifiedTime()?.toMillis() ) {
            if( !ts[0] ) {
                log.trace "Required sibling file not yet visible -- waiting: ${path}"
                ts[0] = System.currentTimeMillis()
                return Boolean.FALSE
            }
            final delta = System.currentTimeMillis() - ts[0]
            if( delta < timeoutMillis )
                return Boolean.FALSE
            log.warn "Required sibling file not visible after ${delta}ms: ${path.toUriString()}"
            return null
        }
        if( !path.text?.trim() ) {
            if( !ts[1] ) {
                log.trace "Required sibling file is empty -- waiting: ${path}"
                ts[1] = System.currentTimeMillis()
                return Boolean.FALSE
            }
            final delta = System.currentTimeMillis() - ts[1]
            if( delta < timeoutMillis )
                return Boolean.FALSE
            log.warn "Required sibling file still empty after ${delta}ms: ${path.toUriString()}"
            return null
        }
        return Boolean.TRUE
    }
}
