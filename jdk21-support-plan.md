# JDK 21 Support Plan

## Top-Level Overview

**Goal:** the code builds and all tests pass on JDK 21 (non-default JDK; `java.default` stays 11).

**Approach:** treat JDK 21 as part of the "17 family" of JVM configuration, not as a copy of the JDK 22 proof-of-concept config.
JDK 22 config (`conf/jvm22-*.options`, `_jvm22_*` in `build.xml`) drops production GC tuning and several exports that the JDK 17 config has (confirmed by Alice by diffing the files), so it must not be reused for 21.

**Key findings (verified in the repo):**
- `build.xml:47` `java.supported="11,17,22"`; `build.xml:224-226` fails the build immediately on any JDK not in the list, so 21 is rejected today.
- `java-jvmargs` (`build.xml:280,322,394`) and `_std-test-jvmargs` (`build.xml:419-439`) are set only for 11/17/22, so on 21 no JVM args would be set. `build.xml:434-436` is a redundant duplicate of the JDK 11 condition.
- `bin/cassandra.in.sh:125-136` actively rejects JDK 21 (`elif [ "$JVM_VERSION" \< "22" ]` -> `exit 1`). `tools/bin/cassandra.in.sh:93-106` has similar misleading logic. `redhat/cassandra.in.sh:130-132,159-161` only knows 11/17.
- Option-file selection (`bin/cassandra.in.sh:159-165`) already maps `>=17` to `jvm17-*.options`, which already contain `-Djava.security.manager=allow` and `--add-modules jdk.incubator.vector`.
- `_jvm17_test_arg_items` (`build.xml:415-417`) lacks `-Djava.security.manager=allow` (mandatory on JDK 18+ for `ThreadAwareSecurityManager`, `src/java/org/apache/cassandra/security/ThreadAwareSecurityManager.java:110`) and `--add-modules jdk.incubator.vector` (needed by jvector SIMD).
- `asm.version=9.4` (`build.xml:177`) cannot read JDK 21 class files (v65; need >= 9.5). Used by simulator (`test/simulator/asm/**`) and `UDFByteCodeVerifier`.
- `.build/run-tests.sh:318` and `.build/run-python-dtests.sh:112` reject upgrade tests on JDK 17 only.
- CI/packaging only reference 11/17: `.circleci/*`, `.github/workflows/code-check.yaml`, `.jenkins/Jenkinsfile:254`, `debian/control`, `redhat/cassandra.spec`, `.build/docker/*.docker`, `README.asc`.
- Only JDK 25 is installed locally (`/usr/lib/jvm/java-25-openjdk`). JDK 25 must NOT be used for the baseline (SecurityManager removed in 24, `Subject.getSubject` throws, Unsafe warnings in 23+).
- Prior art in git: commit `cb5399dbe1` "Fix IllegalAccessException in ControllerTest under JDK 21+" and the comment at `build.xml:555` about "jdk21/2 POC branches" indicate partial earlier work on 21.

**Non-goals / explicitly out of scope:**
- Changing `java.default` (product decision).
- Making the simulator work on JDK 17/21/22 (`InterceptAgent.java:288-291` only supports `11.` and `1.8`); stays restricted to the default JDK as in `.jenkins/Jenkinsfile:254`.
- Changing `FBUtilities.ASM_BYTECODE_VERSION` (`Opcodes.ASM9`) or simulator `BYTECODE_VERSION` (`Opcodes.ASM7`): the ASM `api` argument gates visitor features; the readable class version is governed by the ASM library version.
- JDK 22 CI/test greening and any change to JDK 22 configuration (explicit user decision).

---

## Sub-Tasks

### 1. Enable JDK 21 in the build (minimal, independently reviewable)
**Intent:** allow the build and the test JVMs to start on JDK 21 with the correct JVM arguments.

**Expected Outcomes:**
- `ant jar` starts on JDK 21 without "Unsupported JDK version".
- `java-jvmargs` and `_std-test-jvmargs` are defined on JDK 21.
- JDK 17 test JVM args also gain the security-manager and vector flags (also regression-tested on 17).

**Todo List:**
1. `build.xml:47`: set `java.supported` to `11,17,21,22`.
2. `build.xml:322-324`: make the `java-jvmargs` condition for `_jvm17_args_concat` an `<or>` of ant.java.version 17 and 21.
3. Replace `_jvm17_test_arg_items` with a shared 17+ set: `-Dio.netty.tryReflectionSetAccessible=true`, `-Djava.security.manager=allow`, `--add-modules jdk.incubator.vector`; select it for 17 and 21 via `<or>`. Do not add `-Dnet.bytebuddy.experimental=true` (ByteBuddy 1.14.17 supports 21).
4. Delete the duplicate JDK 11 `_std-test-jvmargs` condition at `build.xml:434-436`.
5. `bin/cassandra.in.sh:128-136`: remove the `elif [ "$JVM_VERSION" \< "22" ]` exit branch and misleading message so 21 maps to `JAVA_VERSION=17` (jvm17 option files). Keep the JDK 11 check and the `< 17` check.
6. `tools/bin/cassandra.in.sh:93-106`: remove the no-op echo branches / align the logic identically.
7. `redhat/cassandra.in.sh:130-132,159-161`: add the `>=17` fallthrough so 21 maps to jvm17 (check that file for matching option-file names first).
8. Verify no change for 11 and 22 behavior.

**Relevant Context:** `build.xml:41-47,224-228,239-439`; `bin/cassandra.in.sh:119-178`; `tools/bin/cassandra.in.sh:87-145`; `redhat/cassandra.in.sh`; `conf/jvm17-server.options`.

**Status:** [x] done
- Implemented: 21 maps to `JAVA_VERSION=17` (jvm17 option files) in `bin/cassandra.in.sh` and `tools/bin/cassandra.in.sh`; `redhat/cassandra.in.sh` needed no change (anything >=17 already maps to 17).
- The duplicate JDK 11 `_std-test-jvmargs` condition (after the `_jvm22_test_arg_items` block) was left untouched because it is next to the JDK 22 block (user: do not touch 22); it is harmless.
- Verified with sdkman JDKs: `ant jar` OK on 21; `UFSecurityTest` passes on 11, 17 and 21 (use the fully qualified name with `-Dtest.name=`, e.g. `org.apache.cassandra.cql3.validation.entities.UFSecurityTest`; short names give ClassNotFoundException). `UFJavaTest` passes on 21 (20 tests).
- Switching JDKs requires `ant realclean` (class files from 21 can't be loaded by 11/17).
- Commit messages must be prefixed with `HCD-645`.

---

### 2. Provision JDK 21 and capture a baseline
**Intent:** get real data on what fails on 21 instead of guessing.

**Expected Outcomes:**
- A documented list of compile errors/warnings and failing tests on JDK 21, versus JDK 17/11 on the same commit.

**Todo List:**
1. Install JDK 21 (e.g. `openjdk-21-jdk` or sdkman Temurin 21) and set `JAVA_HOME`. Confirm `ant -diagnostics` reports `ant.java.version` = 21 (Ant >= 1.10.13 required).
2. `ant clean jar` on JDK 21; record compile warnings/errors.
3. Run `ant check` (checkstyle, RAT, eclipse-warnings).
4. Run `ant test` (unit), then `ant test-jvm-dtest-some` subset, `ant test-burn`/`test-cdc`/`test-compression` as time allows (these use `_std-test-jvmargs`/`java-jvmargs`).
5. Check the log for `jdk.incubator.vector` warnings to confirm the jvector SIMD path is active.
6. Look at git history for earlier JDK 21 work (`git log --all -i --grep=jdk21`, commit `cb5399dbe1`) and reuse any failure notes.
7. Write the failure list into the "Baseline results" section at the bottom of this file, grouped by root cause.

**Relevant Context:** `build.xml:1358-1430` (test macro), `build.xml:555` (antlr fork comment), `TESTING.md`.

**Status:** [ ] pending

---

### 3. Upgrade ASM 9.4 -> 9.7.1 (can run in parallel to task 2)
**Intent:** ASM must be able to read class files version 65 (JDK 21 JDK classes are read by the simulator, user classes by UDF verifier).

**Expected Outcomes:**
- `asm.version` is >= 9.5 (target 9.7.1); build, UDF tests (`UDFByteCodeVerifier`), and simulator-asm still compile.

**Todo List:**
1. `build.xml:177`: bump `asm.version`.
2. Check `.build/parent-pom-template.xml:1098-1122` and any dependency checksum / license / OWASP suppression files that mention the ASM version (grep for `asm-9.4`, `lib/licenses`).
3. Run UDF tests (`UDFunctionTest`, `UDFByteCodeVerifier` related) and `ant simulator-asm-build`.
4. Leave `FBUtilities.ASM_BYTECODE_VERSION` and simulator `BYTECODE_VERSION` unchanged (see non-goals).

**Relevant Context:** `build.xml:174-177,780-810,909-935`; `src/java/org/apache/cassandra/utils/FBUtilities.java:160`; `src/java/org/apache/cassandra/cql3/functions/UDFByteCodeVerifier.java`; `test/simulator/asm/**`.

**Status:** [x] done
- `asm.version` bumped to 9.7.1 (`build.xml:177`). No other references except the generated/stale `ide/nbproject/project.xml` (left alone) and no `lib/licenses` directory in this repo.
- Verified on JDK 21: `ant jar simulator-jars` OK; `UFVerifierTest` 20/20 pass.

---

### 4. Fix failures found on JDK 21
**Intent:** make the whole suite green on 21 without regressing 11/17.

**Expected Outcomes:**
- All unit, in-JVM dtest and other suites pass on JDK 21.

**Todo List (refined after task 2; expected categories):**
1. Missing `--add-opens`/`--add-exports` (typically jamm `measureDeep` or in-JVM dtest isolated class loaders): add to `conf/jvm17-*.options` and `_jvm17_arg_items` in `build.xml` (harmless on 17 if the package exists).
2. CLDR 42 (JDK 20+) differences: tests comparing formatted date/time/number strings (e.g. U+202F before AM/PM). Fix tests with explicit patterns/locales; do not use `-Djava.locale.providers=COMPAT`.
3. Reflection/Unsafe/ByteBuddy/Mockito related test failures (e.g. like `ControllerTest` fix in `cb5399dbe1`).
4. JDK-behaviour differences in tests (TLS defaults, Thread API, `Runtime`/GC beans, timing).
5. Re-run affected suites on 11 and 17 after each fix.
6. Keep each fix as a separate small change grouped by root cause.

**Relevant Context:** `conf/jvm17-server.options`, `conf/jvm17-clients.options`, `build.xml:284-320`, `test/`.

**Status:** [ ] pending

---

### 5. Fix scripts, CI, packaging and docs
**Intent:** make 21 a usable supported JDK across tooling and documentation.

**Expected Outcomes:**
- Scripts and CI definitions accept/test JDK 21; packaging dependencies allow it; docs mention it.

**Todo List:**
1. `.build/run-tests.sh:318` and `.build/run-python-dtests.sh:112`: extend the "upgrade tests only on overlapping JDK" check to JDK 21 (and 22).
2. `.build/docker/*.docker` (`ubuntu-test.docker`, `almalinux-build.docker`, `bullseye-build.docker`): ensure `openjdk-21` is installed so `.build/docker/_set_java.sh` (`update-java-alternatives`/`alternatives`) can switch to it.
3. `.github/workflows/code-check.yaml`: add a JDK 21 job (and optionally the CircleCI executor in `.circleci/config_template.yml`, regenerating via `.circleci/generate.sh`).
4. `.jenkins/Jenkinsfile:254`: keep simulator-dtest/cqlsh/upgrade restricted to the default JDK; verify the matrix with 21 added through `java.supported`.
5. `debian/control` and `redhat/cassandra.spec`: allow `openjdk-21` / `java-21-headless` alternatives.
6. `README.asc`, `CHANGES.txt`, `NEWS.txt` (user-facing note: JDK 21 supported as non-default), optionally `doc/modules/cassandra/pages/reference/java17.adoc`.
7. Check `.build/run-ci.d/README.md`, `.build/ci/junit_helpers.py` for JDK enumerations.

**Relevant Context:** files above; `.build/docker/_set_java.sh:31-65`.

**Status:** [ ] pending

---

### 6. Final validation
**Intent:** prove the end state.

**Expected Outcomes:**
- Green runs on JDK 21; no regressions on JDK 11 and 17.

**Todo List:**
1. JDK 21: `ant clean jar`, `ant check`, `ant test`, in-JVM dtests (`test/distributed`, most likely to expose missing add-opens), `ant artifacts`/`ant javadoc` (stricter doclint), `ant generate-idea-files`.
2. JDK 17 and JDK 11: `ant clean jar` and `ant test` (17 regression is mandatory because its test JVM args changed in sub-task 1).
3. Start a node with `bin/cassandra` on JDK 21 and confirm JVM options (`jvm17-server.options`) are applied.
4. Report remaining known issues; decide with the team which support tier JDK 21 gets (experimental vs. fully supported).

**Status:** [ ] pending

---

## Baseline results (to be filled in by sub-task 2)

_Empty until a JDK 21 run is available._
i/junit_helpers.py` for JDK enumerations.

**Relevant Context:** files above; `.build/docker/_set_java.sh:31-65`.

**Status:** [ ] pending

---

### 6. Final validation
**Intent:** prove the end state.

**Expected Outcomes:**
- Green runs on JDK 21; no regressions on JDK 11 and 17.

**Todo List:**
1. Local, JDK 21: `ant clean jar`, `ant check`, `ant artifacts`/`ant javadoc` (stricter doclint), `ant generate-idea-files`, plus targeted tests for every changed area only.
2. Local, JDK 17 and 11: `ant clean jar` and a few targeted tests (17 regression check is needed because its test JVM args changed in sub-task 1).
3. Start a node with `bin/cassandra` on JDK 21 and confirm JVM options (`jvm17-server.options`) are applied.
4. Push and let CI run the full suites on JDK 21 (unit, in-JVM dtests `test/distributed`, burn, cdc, compression) and on 17/11 for regression; triage CI failures iteratively through sub-task 4.
5. Report remaining known issues; decide with the team which support tier JDK 21 gets (experimental vs. fully supported).

**Status:** [ ] pending

---

## Baseline results (to be filled in by sub-task 2)

_Empty until a JDK 21 run is available._
