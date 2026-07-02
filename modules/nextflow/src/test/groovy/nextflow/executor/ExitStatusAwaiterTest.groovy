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

import java.nio.file.Files

import nextflow.util.Duration
import spock.lang.Specification

class ExitStatusAwaiterTest extends Specification {

    // --- exit file only (no siblings) ---

    def 'should defer when exit file is absent within timeout'() {
        given:
        def dir = Files.createTempDirectory('nf-esa-test')
        def exitFile = dir.resolve('.exitcode')
        def awaiter = new ExitStatusAwaiter(Duration.of('2sec'))

        expect:
        awaiter.read(exitFile) == null

        cleanup:
        dir.toFile().deleteDir()
    }

    def 'should return MAX_VALUE when exit file stays absent past timeout'() {
        given:
        def dir = Files.createTempDirectory('nf-esa-test')
        def exitFile = dir.resolve('.exitcode')
        def awaiter = new ExitStatusAwaiter(Duration.of('100ms'))

        when: 'first call starts the missing-file clock'
        def r1 = awaiter.read(exitFile)
        then:
        r1 == null

        when: 'second call after timeout elapsed'
        sleep(150)
        def r2 = awaiter.read(exitFile)
        then:
        r2 == Integer.MAX_VALUE

        cleanup:
        dir.toFile().deleteDir()
    }

    def 'should parse exit code once exit file appears'() {
        given:
        def dir = Files.createTempDirectory('nf-esa-test')
        def exitFile = dir.resolve('.exitcode')
        def awaiter = new ExitStatusAwaiter(Duration.of('2sec'))

        when: 'file absent on first poll'
        def r1 = awaiter.read(exitFile)
        then:
        r1 == null

        when: 'file written with exit code 42'
        exitFile.text = '42'
        def r2 = awaiter.read(exitFile)
        then:
        r2 == 42

        cleanup:
        dir.toFile().deleteDir()
    }

    // --- sibling gating ---

    def 'should defer when exit file present but sibling not yet visible'() {
        given:
        def dir = Files.createTempDirectory('nf-esa-test')
        def exitFile = dir.resolve('.exitcode')
        def sibling  = dir.resolve('.command.env')
        exitFile.text = '0'
        // sibling deliberately not written yet
        def awaiter = new ExitStatusAwaiter(Duration.of('2sec'), [sibling])

        expect:
        awaiter.read(exitFile) == null

        cleanup:
        dir.toFile().deleteDir()
    }

    def 'should return exit code once sibling becomes visible'() {
        given:
        def dir = Files.createTempDirectory('nf-esa-test')
        def exitFile = dir.resolve('.exitcode')
        def sibling  = dir.resolve('.command.env')
        exitFile.text = '0'
        def awaiter = new ExitStatusAwaiter(Duration.of('2sec'), [sibling])

        when: 'sibling absent — defer'
        def r1 = awaiter.read(exitFile)
        then:
        r1 == null

        when: 'sibling appears with content'
        sibling.text = 'MY_VAR=hello'
        def r2 = awaiter.read(exitFile)
        then:
        r2 == 0

        cleanup:
        dir.toFile().deleteDir()
    }

    def 'should defer when sibling is present but empty'() {
        given:
        def dir = Files.createTempDirectory('nf-esa-test')
        def exitFile = dir.resolve('.exitcode')
        def sibling  = dir.resolve('.command.env')
        exitFile.text = '0'
        sibling.text  = ''   // exists but empty
        def awaiter = new ExitStatusAwaiter(Duration.of('2sec'), [sibling])

        expect:
        awaiter.read(exitFile) == null

        cleanup:
        dir.toFile().deleteDir()
    }

    def 'should return MAX_VALUE when sibling stays absent past timeout'() {
        given:
        def dir = Files.createTempDirectory('nf-esa-test')
        def exitFile = dir.resolve('.exitcode')
        def sibling  = dir.resolve('.command.env')
        exitFile.text = '0'
        def awaiter = new ExitStatusAwaiter(Duration.of('100ms'), [sibling])

        when: 'first call — sibling missing, start clock'
        def r1 = awaiter.read(exitFile)
        then:
        r1 == null

        when: 'second call after timeout'
        sleep(150)
        def r2 = awaiter.read(exitFile)
        then:
        r2 == Integer.MAX_VALUE

        cleanup:
        dir.toFile().deleteDir()
    }

    def 'reset() should clear sibling timestamps so clock restarts'() {
        given:
        def dir = Files.createTempDirectory('nf-esa-test')
        def exitFile = dir.resolve('.exitcode')
        def sibling  = dir.resolve('.command.env')
        exitFile.text = '0'
        def awaiter = new ExitStatusAwaiter(Duration.of('100ms'), [sibling])

        when: 'consume the timeout'
        awaiter.read(exitFile)
        sleep(150)
        def r1 = awaiter.read(exitFile)
        then:
        r1 == Integer.MAX_VALUE

        when: 'reset and poll again — clock should restart, sibling still absent'
        awaiter.reset()
        def r2 = awaiter.read(exitFile)
        then:
        r2 == null  // deferred again, not immediately MAX_VALUE

        cleanup:
        dir.toFile().deleteDir()
    }

    // --- envSiblings factory ---

    def 'envSiblings returns empty list when task has no env outputs'() {
        given:
        def task = Mock(nextflow.processor.TaskRun) {
            getOutputs() >> [:]
        }

        expect:
        ExitStatusAwaiter.envSiblings(task).isEmpty()
    }

    def 'envSiblings returns command.env path when task has EnvOutParam'() {
        given:
        def workDir = Files.createTempDirectory('nf-esa-test')
        def envParam = Mock(nextflow.script.params.EnvOutParam)
        def task = Mock(nextflow.processor.TaskRun) {
            getOutputs() >> [(envParam): 'MY_VAR']
            getWorkDir() >> workDir
        }

        when:
        def siblings = ExitStatusAwaiter.envSiblings(task)

        then:
        siblings.size() == 1
        siblings[0] == workDir.resolve(nextflow.processor.TaskRun.CMD_ENV)

        cleanup:
        workDir.toFile().deleteDir()
    }

    def 'envSiblings returns command.env path when task has CmdEvalParam'() {
        given:
        def workDir = Files.createTempDirectory('nf-esa-test')
        def evalParam = Mock(nextflow.script.params.CmdEvalParam)
        def task = Mock(nextflow.processor.TaskRun) {
            getOutputs() >> [(evalParam): 'echo hi']
            getWorkDir() >> workDir
        }

        when:
        def siblings = ExitStatusAwaiter.envSiblings(task)

        then:
        siblings.size() == 1
        siblings[0] == workDir.resolve(nextflow.processor.TaskRun.CMD_ENV)

        cleanup:
        workDir.toFile().deleteDir()
    }
}
