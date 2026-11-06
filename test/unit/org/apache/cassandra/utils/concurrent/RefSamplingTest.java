/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils.concurrent;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.shared.WithProperties;

import static org.apache.cassandra.config.CassandraRelevantProperties.DEBUG_REF_COUNT_COPY_SAMPLE_INTERVAL;
import static org.apache.cassandra.config.CassandraRelevantProperties.DEBUG_REF_COUNT_PRIMARY_SAMPLE_INTERVAL;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_DEBUG_REF_COUNT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Sampled {@link Ref} debug records: {@code cassandra.debugrefcount} off, and both sample intervals on. Unit tests
 * run with {@code cassandra.debugrefcount=true}, and {@code Ref} reads the properties once, when it is initialised,
 * so this class sets them and then initialises {@code Ref} in its static initialiser, and
 * {@link #configurationTookEffect()} fails if {@code Ref} was loaded earlier. Each test class runs in its own JVM.
 * <p>
 * The intervals are 2 for the references {@code new Ref(referent, tidy)} makes and 4 for their copies, so that the
 * same JVM creates sampled and unsampled references of both kinds through the production path, and a mix-up of the
 * two intervals shows. Other intervals are tested through {@link Ref#sampled(int)}, which the production path calls.
 */
public class RefSamplingTest
{
    private static final int PRIMARY_INTERVAL = 2;
    private static final int COPY_INTERVAL = 4;

    static
    {
        TEST_DEBUG_REF_COUNT.setBoolean(false);
        DEBUG_REF_COUNT_PRIMARY_SAMPLE_INTERVAL.setInt(PRIMARY_INTERVAL);
        DEBUG_REF_COUNT_COPY_SAMPLE_INTERVAL.setInt(COPY_INTERVAL);
        // initialise Ref now, as some tests below change the properties
        Ref.sampled(0);
    }

    private static final int REFS = 100_000;
    private static final String LEAK_SUFFIX = "was not released before the reference was garbage collected";
    private static final String FRAME = "\tat " + RefSamplingTest.class.getName() + '.';

    private final Logger refLogger = (Logger) LoggerFactory.getLogger(Ref.class);
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    private final Set<String> leaked = ConcurrentHashMap.newKeySet();
    private Ref.OnLeak previousOnLeak;

    private static final class Tidier implements RefCounted.Tidy
    {
        private final String name;

        Tidier(String name)
        {
            this.name = name;
        }

        public void tidy()
        {
        }

        public String name()
        {
            return name;
        }
    }

    @Before
    public void captureLogsAndLeaks()
    {
        appender.start();
        refLogger.addAppender(appender);
        // installed before any test creates a reference, so that no leak report can be missed
        previousOnLeak = Ref.ON_LEAK;
        Ref.ON_LEAK = state -> leaked.add(state.toString());
    }

    @After
    public void stopCapturing()
    {
        Ref.ON_LEAK = previousOnLeak;
        refLogger.detachAppender(appender);
        appender.stop();
    }

    @Test
    public void configurationTookEffect()
    {
        assertThat(Ref.DEBUG_ENABLED).isFalse();
        assertThat(Ref.PRIMARY_SAMPLE_INTERVAL).isEqualTo(PRIMARY_INTERVAL);
        assertThat(Ref.COPY_SAMPLE_INTERVAL).isEqualTo(COPY_INTERVAL);
    }

    @Test
    public void samplingDoesNotStartStrongLeakDetector()
    {
        assertThat(Ref.PRIMARY_SAMPLE_INTERVAL).isPositive();
        assertThat(Ref.COPY_SAMPLE_INTERVAL).isPositive();
        assertThat(Ref.STRONG_LEAK_DETECTOR).isNull();
        List<String> threads = Thread.getAllStackTraces().keySet().stream()
                                     .map(Thread::getName)
                                     .collect(Collectors.toList());
        assertThat(threads).noneMatch(name -> name.startsWith("Strong-Reference-Leak-Detector"));
    }

    @Test
    public void intervalOneSamplesEveryReference()
    {
        for (int i = 0; i < REFS; i++)
            assertThat(Ref.sampled(1)).isTrue();
    }

    @Test
    public void intervalZeroSamplesNothing()
    {
        for (int i = 0; i < REFS; i++)
            assertThat(Ref.sampled(0)).isFalse();
    }

    @Test
    public void intervalSamplesOneInN()
    {
        int sampled = 0;
        for (int i = 0; i < REFS; i++)
        {
            if (Ref.sampled(64))
                sampled++;
        }
        // expected 1562.5, standard deviation about 39
        assertThat(sampled).isBetween(1200, 1900);
    }

    @Test
    public void configuredIntervalsSampleOneInNReferencesAndCopies()
    {
        Tidier tidier = new Tidier("fraction");
        int sampled = 0;
        for (int i = 0; i < REFS; i++)
        {
            Ref<?> ref = new Ref<>(null, tidier);
            if (ref.state.debug != null)
                sampled++;
            ref.release();
        }
        // 1 in 2: expected 50000, standard deviation about 158
        assertThat(sampled).isBetween(48_000, 52_000);

        Ref<?> primary = new Ref<>(null, tidier);
        int sampledCopies = 0;
        for (int i = 0; i < REFS; i++)
        {
            Ref<?> copy = i % 2 == 0 ? primary.ref() : primary.tryRef();
            assertThat(copy.state).isNotInstanceOf(Ref.PrimaryState.class);
            if (copy.state.debug != null)
                sampledCopies++;
            copy.release();
        }
        primary.release();
        // 1 in 4: expected 25000, standard deviation about 137
        assertThat(sampledCopies).isBetween(23_000, 27_000);
    }

    @Test
    public void sampledLeakLogsAllocationTrace()
    {
        String name = "sampled leak";
        leakSampledRef(name);

        awaitLeak(name);
        assertThat(onlyMessage(events(Level.ERROR, "LEAK DETECTED", name))).endsWith(LEAK_SUFFIX);
        assertThat(onlyMessage(events(Level.ERROR, "Allocate trace", name))).contains(Thread.currentThread().toString())
                                                                            .contains(FRAME + "leakSampledRef(");
    }

    private static void leakSampledRef(String name)
    {
        createRef(name, true);
    }

    @Test
    public void sampledCopyLeakLogsAllocationTrace()
    {
        String name = "sampled copy leak";
        leakSampledCopy(name);

        awaitLeak(name);
        assertThat(onlyMessage(events(Level.ERROR, "LEAK DETECTED", name))).endsWith(LEAK_SUFFIX);
        assertThat(onlyMessage(events(Level.ERROR, "Allocate trace", name))).contains(FRAME + "leakSampledCopy(");
    }

    private static void leakSampledCopy(String name)
    {
        createCopy(name, true);
    }

    @Test
    public void unsampledLeakLogsHintNamingPrimaryInterval()
    {
        String name = "unsampled leak";
        createRef(name, false);

        awaitLeak(name);
        assertThat(onlyMessage(events(Level.ERROR, "LEAK DETECTED", name)))
            .startsWith("LEAK DETECTED: a reference (")
            .endsWith(LEAK_SUFFIX + "; no allocation stack, as only 1 in 2 references is sampled " +
                      "(changing -Dcassandra.debugrefcount.primary_sample_interval needs a restart)");
        assertThat(events(Level.ERROR, "Allocate trace", name)).isEmpty();
    }

    @Test
    public void unsampledCopyLeakLogsHintNamingCopyInterval()
    {
        String name = "unsampled copy leak";
        createCopy(name, false);

        awaitLeak(name);
        assertThat(onlyMessage(events(Level.ERROR, "LEAK DETECTED", name)))
            .endsWith(LEAK_SUFFIX + "; no allocation stack, as only 1 in 4 copied references is sampled " +
                      "(changing -Dcassandra.debugrefcount.copy_sample_interval needs a restart)");
        assertThat(events(Level.ERROR, "Allocate trace", name)).isEmpty();
    }

    @Test
    public void hintWhenSamplingIsOffSaysHowToTurnItOn()
    {
        assertThat(Ref.noDebugHint(true, 0))
            .isEqualTo("; no allocation stack, as sampling of references is off " +
                       "(restart with -Dcassandra.debugrefcount.primary_sample_interval=N to sample 1 in N)");
        assertThat(Ref.noDebugHint(false, 0))
            .isEqualTo("; no allocation stack, as sampling of copied references is off " +
                       "(restart with -Dcassandra.debugrefcount.copy_sample_interval=N to sample 1 in N)");
    }

    @Test
    public void sampledReleaseLogsNoTraces()
    {
        createRef("sampled release", true).release();
        createRef("sampled close", true).close();
        createCopy("sampled copy release", true).release();
        createCopy("sampled copy close", true).close();

        // no name filter, so that a trace logged under another id, or none, is caught too
        assertThat(events(Level.ERROR, "trace", null)).isEmpty();
    }

    @Test
    public void sampledDoubleReleaseLogsBothTraces()
    {
        String name = "sampled double release";
        Ref<?> ref = createRef(name, true);
        releaseOnce(ref);

        assertThatThrownBy(ref::release).isInstanceOf(IllegalStateException.class)
                                        .hasMessage("Attempted to release a reference that has already been released");

        assertThat(events(Level.ERROR, "BAD RELEASE", name)).hasSize(1);
        assertThat(onlyMessage(events(Level.ERROR, "Allocate trace", name))).contains(FRAME + "createRef(");
        assertThat(onlyMessage(events(Level.ERROR, "Deallocate trace", name))).contains(FRAME + "releaseOnce(");
    }

    private static void releaseOnce(Ref<?> ref)
    {
        ref.release();
    }

    @Test
    public void unsampledDoubleReleaseLogsNoTraces()
    {
        String name = "unsampled double release";
        Ref<?> ref = createRef(name, false);
        ref.release();

        assertThatThrownBy(ref::release).isInstanceOf(IllegalStateException.class);

        assertThat(events(Level.ERROR, "BAD RELEASE", name)).hasSize(1);
        assertThat(events(Level.ERROR, "trace", name)).isEmpty();
    }

    @Test
    public void printDebugInfoNamesReleasingThreadOfSampledReference()
    {
        Ref<?> sampled = createRef("sampled print", true);
        sampled.release();
        assertThat(sampled.printDebugInfo()).isEqualTo("Memory was freed by " + Thread.currentThread());

        Ref<?> closed = createRef("sampled print after close", true);
        closed.close();
        assertThat(closed.printDebugInfo()).isEqualTo("Memory was freed by " + Thread.currentThread());

        Ref<?> unsampled = createRef("unsampled print", false);
        unsampled.release();
        assertThat(unsampled.printDebugInfo()).isEqualTo("Memory was freed");
    }

    @Test
    public void sampledUseAfterReleaseLogsTracesOnce()
    {
        String name = "sampled use after release";
        Ref<?> ref = createRef(name, true);
        ref.release();

        // with assertions off, these would return; either way, the record is logged only once
        for (int i = 0; i < 3; i++)
            assertThatThrownBy(ref::get).isInstanceOf(AssertionError.class);

        assertThat(events(Level.ERROR, "Allocate trace", name)).hasSize(1);
        assertThat(events(Level.ERROR, "Deallocate trace", name)).hasSize(1);
    }

    @Test
    public void deallocateTraceLoggedEvenIfAllocateTraceWasLoggedBeforeRelease()
    {
        String name = "sampled logged before release";
        Ref<?> ref = createRef(name, true);
        // as a use racing with the release would, before the release records its trace
        ref.printDebugInfo();
        assertThat(events(Level.ERROR, "Allocate trace", name)).hasSize(1);
        assertThat(events(Level.ERROR, "Deallocate trace", name)).isEmpty();

        ref.release();
        // logged by the release itself, as no later use may log the record again
        assertThat(events(Level.ERROR, "Deallocate trace", name)).hasSize(1);
        assertThatThrownBy(ref::get).isInstanceOf(AssertionError.class);
        assertThatThrownBy(ref::get).isInstanceOf(AssertionError.class);

        assertThat(events(Level.ERROR, "Allocate trace", name)).hasSize(1);
        assertThat(onlyMessage(events(Level.ERROR, "Deallocate trace", name))).contains(FRAME + "deallocateTraceLoggedEvenIfAllocateTraceWasLoggedBeforeRelease(");
    }

    // the same sequence as deallocateTraceLoggedEvenIfAllocateTraceWasLoggedBeforeRelease, entered through
    // debug.log rather than printDebugInfo
    @Test
    public void deallocateTraceLoggedOnReleaseIfLosingReleaseLoggedAllocateTrace()
    {
        String name = "sampled logged by losing release";
        Ref<?> ref = createRef(name, true);
        // as the losing release of a concurrent double release does before the winning one records its trace;
        // nothing logs the record after that
        ref.state.debug.log(ref.state.toString());
        assertThat(events(Level.ERROR, "Allocate trace", name)).hasSize(1);
        assertThat(events(Level.ERROR, "Deallocate trace", name)).isEmpty();

        releaseOnce(ref);

        assertThat(events(Level.ERROR, "Allocate trace", name)).hasSize(1);
        assertThat(onlyMessage(events(Level.ERROR, "Deallocate trace", name))).contains(FRAME + "releaseOnce(");
    }

    @Test
    public void invalidIntervalTurnsSamplingOffWithOneWarning()
    {
        for (CassandraRelevantProperties property : new CassandraRelevantProperties[]{ DEBUG_REF_COUNT_PRIMARY_SAMPLE_INTERVAL, DEBUG_REF_COUNT_COPY_SAMPLE_INTERVAL })
        {
            for (String value : new String[]{ "-1", "abc", "", "1.5" })
            {
                appender.list.clear();
                try (WithProperties properties = new WithProperties().set(property, value))
                {
                    assertThat(Ref.sampleInterval(property)).as(value).isZero();
                }
                List<ILoggingEvent> warnings = events(Level.WARN, property.getKey(), null);
                assertThat(warnings).as(value).hasSize(1);
                assertThat(warnings.get(0).getFormattedMessage())
                    .isEqualTo("Invalid value '" + value + "' for " + property.getKey() + ", expected an integer >= 0; this Ref debug sampling is off");
            }
        }
    }

    @Test
    public void validIntervalIsReadWithoutWarning()
    {
        for (CassandraRelevantProperties property : new CassandraRelevantProperties[]{ DEBUG_REF_COUNT_PRIMARY_SAMPLE_INTERVAL, DEBUG_REF_COUNT_COPY_SAMPLE_INTERVAL })
        {
            try (WithProperties properties = new WithProperties().set(property, "0"))
            {
                assertThat(Ref.sampleInterval(property)).isZero();
            }
            try (WithProperties properties = new WithProperties().set(property, "4096"))
            {
                assertThat(Ref.sampleInterval(property)).isEqualTo(4096);
            }
            try (WithProperties properties = new WithProperties().clear(property))
            {
                assertThat(Ref.sampleInterval(property)).isEqualTo(Integer.parseInt(property.getDefaultValue()));
            }
        }
        assertThat(DEBUG_REF_COUNT_PRIMARY_SAMPLE_INTERVAL.getDefaultValue()).isEqualTo("64");
        assertThat(DEBUG_REF_COUNT_COPY_SAMPLE_INTERVAL.getDefaultValue()).isEqualTo("2048");
        assertThat(events(Level.WARN, "", null)).isEmpty();
    }

    /**
     * Creates references until one is, or is not, sampled, and returns it; releases the others.
     */
    private static Ref<?> createRef(String name, boolean sampled)
    {
        for (int i = 0; i < 1000; i++)
        {
            Ref<?> ref = new Ref<>(null, new Tidier(name));
            assertThat(ref.state).isInstanceOf(Ref.PrimaryState.class);
            if ((ref.state.debug != null) == sampled)
                return ref;
            ref.release();
        }
        throw new AssertionError("no " + (sampled ? "sampled" : "unsampled") + " reference in 1000 at interval " + Ref.PRIMARY_SAMPLE_INTERVAL);
    }

    /**
     * Copies a new reference until a copy is, or is not, sampled, and returns that copy; releases the reference and
     * the other copies.
     */
    private static Ref<?> createCopy(String name, boolean sampled)
    {
        Ref<?> primary = new Ref<>(null, new Tidier(name));
        try
        {
            for (int i = 0; i < 1000; i++)
            {
                Ref<?> copy = primary.ref();
                if ((copy.state.debug != null) == sampled)
                    return copy;
                copy.release();
            }
        }
        finally
        {
            primary.release();
        }
        throw new AssertionError("no " + (sampled ? "sampled" : "unsampled") + " copy in 1000 at interval " + Ref.COPY_SAMPLE_INTERVAL);
    }

    /**
     * Waits until the reaper has reported the leak of a reference to the tidy called {@code name}. The reaper calls
     * {@link Ref#ON_LEAK} after logging, so every log event of that report is in the appender by then.
     */
    private void awaitLeak(String name)
    {
        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200)).untilAsserted(() -> {
            System.gc();
            assertThat(leaked).anyMatch(id -> id.endsWith(':' + name));
        });
    }

    private static String onlyMessage(List<ILoggingEvent> events)
    {
        assertThat(events).hasSize(1);
        return events.get(0).getFormattedMessage();
    }

    /**
     * The captured events at {@code level} whose message contains {@code text} and, unless {@code name} is null,
     * the id of a reference to the tidy called {@code name}.
     */
    private List<ILoggingEvent> events(Level level, String text, String name)
    {
        List<ILoggingEvent> events;
        // the appender appends under its own lock, from whichever thread logs (the reaper, for leaks)
        synchronized (appender)
        {
            events = new ArrayList<>(appender.list);
        }
        return events.stream()
                     .filter(e -> e.getLevel() == level)
                     .filter(e -> e.getFormattedMessage().contains(text))
                     .filter(e -> name == null || e.getFormattedMessage().contains(':' + name))
                     .collect(Collectors.toList());
    }
}
