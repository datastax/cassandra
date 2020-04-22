/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.guardrails;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.Units;

import static java.lang.String.format;

/**
 * General class defining a given guardrail (that guards against some particular usage/condition).
 *
 * <p>Some guardrails only emit warnings when triggered, while other fail the query that trigger them. Some may do one
 * or the other based on specific threshold.
 *
 * <p>Note that all the defined class support live updates, which is why each guardrail class ctor takes suppliers of
 * the condition the guardrail acts on rather than the condition itself. Which does imply that said suppliers should
 * be fast and non-blocking to avoid surprises. Note that this does not mean live updates are exposed to the user,
 * just that the implementation is up to it if we ever want to expose it.
 */
public abstract class Guardrail
{
    private static final NoSpamLogger logger = NoSpamLogger.getLogger(LoggerFactory.getLogger(Guardrail.class),
                                                                      10, TimeUnit.MINUTES);

    /**
     * A name identifying the guardrail (mainly for shipping with Insights events).
     */
    public final String name;

    protected Guardrail(String name)
    {
        this.name = name;
    }

    protected void warn(String message)
    {
        logger.warn(message);
        // Note that ClientWarn will simply ignore the message if we're not running this as part of a user query
        // (the internal "state" will be null)
        ClientWarn.instance.warn(message);
        for (Guardrails.Listener listener : Guardrails.listeners)
            listener.onWarningTriggered(name, message);
    }

    protected void fail(String message)
    {
        logger.error(message);
        for (Guardrails.Listener listener : Guardrails.listeners)
            listener.onFailureTriggered(name, message);
        throw new InvalidRequestException(message);
    }

    /**
     * A guardrail based on numeric threshold(s).
     *
     * <p>A {@link Threshold} guardrail defines (up to) 2 threshold, one at which a warning is issued, and a higher one
     * at which a failure is triggered. Only one of those threshold can be activated if desired.
     *
     * <p>This guardrail only handles guarding positive values.
     */
    public static class Threshold extends Guardrail
    {
        /**
         * A function used to build the error message of a triggered {@link Threshold} guardrail.
         */
        public interface ErrorMessageProvider
        {
            /**
             * Called when the guardrail is triggered to build the corresponding error message.
             *
             * @param isWarning       whether the trigger is a warning one; otherwise it is failure one.
             * @param what            a string, provided by the call to the {@link #guard} method, describing what the guardrail
             *                        has been applied to (and that has triggered it).
             * @param valueString     the value that triggered the guardrail (as a string).
             * @param thresholdString the threshold that was passed to trigger the guardrail (as a string).
             */
            public String createMessage(boolean isWarning, String what, String valueString, String thresholdString);
        }

        final LongSupplier warnThreshold;
        final LongSupplier failThreshold;
        final ErrorMessageProvider errorMessageProvider;

        /**
         * Creates a new {@link Threshold} guardrail.
         *
         * @param name                 the name of the guardrail (for identification in {@link Guardrails.Listener} events).
         * @param warnThreshold        a supplier of the threshold above which a warning should be triggered. This cannot be
         *                             null, but {@code () -> -1L} can be provided if no warning threshold is desired.
         * @param failThreshold        a supplier of the threshold above which a failure should be triggered. This cannot be
         *                             null, but {@code () -> -1L} can be provided if no failure threshold is desired.
         * @param errorMessageProvider a function to generate the error message if the guardrail is triggered
         *                             (being it for a warning or a failure).
         */
        Threshold(String name,
                  LongSupplier warnThreshold,
                  LongSupplier failThreshold,
                  ErrorMessageProvider errorMessageProvider)
        {
            super(name);
            this.warnThreshold = warnThreshold;
            this.failThreshold = failThreshold;
            this.errorMessageProvider = errorMessageProvider;
        }

        protected String errMsg(boolean isWarning, String what, long value, long thresholdValue)
        {
            return errorMessageProvider.createMessage(isWarning,
                                                      what,
                                                      Long.toString(value),
                                                      Long.toString(thresholdValue));
        }

        private long failValue()
        {
            long failValue = failThreshold.getAsLong();
            return failValue < 0 ? Long.MAX_VALUE : failValue;
        }

        private long warnValue()
        {
            long warnValue = warnThreshold.getAsLong();
            return warnValue < 0 ? Long.MAX_VALUE : warnValue;
        }

        /**
         * Checks whether the provided value would trigger a warning or failure if passed to {@link #guard}.
         *
         * <p>This method is optional (does not have to be called) but can be used in the case where the "what"
         * argument to {@link #guard} is expensive to build to save doing so in the common case (of the guardrail
         * not being triggered).
         *
         * @param value the value to test.
         * @return {@code true} if {@code value} is above the warning or failure thresholds of this guardrail,
         * {@code false otherwise}.
         */
        public boolean triggersOn(long value)
        {
            return Guardrails.enabled() && (value > Math.min(failValue(), warnValue()));
        }

        /**
         * Apply the guardrail to the provided value, triggering a warning or failure if appropriate.
         *
         * @param value the value to check.
         * @param what  a string describing what {@code value} is a value of used in the error message if the
         *              guardrail is triggered (for instance, say the guardrail guards the size of column values, then this
         *              argument must describe which column of which row is triggering the guardrail for convenience). Note that
         *              this is only used if the guardrail triggers, so if it is expensive to build, you can put the call to
         *              this method behind a {@link #triggersOn} call.
         */
        public void guard(long value, String what)
        {
            if (!Guardrails.enabled())
                return;

            long failValue = failValue();
            if (value > failValue)
                fail(errMsg(false, what, value, failValue));

            long warnValue = warnValue();
            if (value > warnValue)
                warn(errMsg(true, what, value, warnValue));
        }
    }

    /**
     * A {@link Threshold} guardrail whose values represent a byte size.
     *
     * <p>This work exactly as a {@link Threshold}, but provides slightly more convenient error messages (display
     * the sizes in human readable format).
     */
    public static class SizeThreshold extends Threshold
    {
        SizeThreshold(String name,
                      LongSupplier warnThreshold,
                      LongSupplier failThreshold,
                      ErrorMessageProvider errorMessageProvider)
        {
            super(name, warnThreshold, failThreshold, errorMessageProvider);
        }

        @Override
        protected String errMsg(boolean isWarning, String what, long value, long thresholdValue)
        {
            return errorMessageProvider.createMessage(isWarning,
                                                      what,
                                                      Units.toString(value, SizeUnit.BYTES),
                                                      Units.toString(thresholdValue, SizeUnit.BYTES));
        }
    }

    /**
     * A guardrail that completely disables the use of a particular feature.
     *
     * <p>Note that this guardrail only triggers failures (if the feature is disabled) so is only meant for
     * query-based guardrails (we're happy to reject queries deemed dangerous, but we don't want to create a guardrail
     * that breaks compaction for instance).
     */
    public static class DisableFlag extends Guardrail
    {
        private final BooleanSupplier disabled;
        private final String what;

        /**
         * Creates a new {@link DisableFlag} guardrail.
         *
         * @param name     the name of the guardrail (for identification in {@link Guardrails.Listener} events).
         * @param disabled a supplier of boolean indicating whether the feature guarded by this guardrail must be
         *                 disabled.
         * @param what     the feature that is guarded by this guardrail (for reporting in error messages).
         */
        DisableFlag(String name, BooleanSupplier disabled, String what)
        {
            super(name);
            this.disabled = disabled;
            this.what = what;
        }

        /**
         * Triggers a failure if this guardrail is disabled.
         *
         * <p>This must be called when the feature guarded by this guardrail is used to ensure such use is in fact
         * allowed.
         */
        public void ensureEnabled()
        {
            if (Guardrails.enabled() && disabled.getAsBoolean())
                fail(what + " is not allowed");
        }
    }

    /**
     * A guardrail that rejects the use of specific values.
     *
     * <p>Note that like {@link DisableFlag}, this guardrail only trigger failures and is thus only for query-based
     * guardrails.
     *
     * @param <T> the type of the values of which certain are disallowed.
     */
    public static class DisallowedValues<T> extends Guardrail
    {
        /*
         * Implementation note: as mentioned in the class Javadoc and for consistency with the other Guardrail
         * implementation of this class (and to generally avoid surprises), this implementation ensures that live
         * changes to the underlying guardrail setting gets reflected. This is the reason for the relative
         * "complexity" of this class.
         */

        private final Supplier<Set<String>> rawSupplier;
        private final Function<String, T> parser;
        private final String what;

        private volatile Set<T> cachedDisallowed;
        private volatile Set<String> cachedRaw;

        /**
         * Creates a new {@link DisallowedValues} guardrail.
         *
         * @param name          the name of the guardrail (for identification in {@link Guardrails.Listener} events).
         * @param disallowedRaw a supplier of the values that are disallowed in raw (string) form. The set returned by
         *                      this supplier <b>must</b> be immutable (we don't use {@code ImmutableSet} because we
         *                      want to feed values from {@link GuardrailsConfig} directly and having ImmutableSet
         *                      there would currently be annoying (because populated automatically by snakeYaml)).
         * @param parser        a function to parse the value to disallow from string.
         * @param what          what represents the value disallowed (for reporting in error messages).
         */
        DisallowedValues(String name, Supplier<Set<String>> disallowedRaw, Function<String, T> parser, String what)
        {
            super(name);
            this.rawSupplier = disallowedRaw;
            this.parser = parser;
            this.what = what;

            ensureUpToDate();
        }

        private void ensureUpToDate()
        {
            Set<String> current = rawSupplier.get();
            // Same as below, this shouldn't happen if settings have been properly sanitized, but throw a meaningful
            // error if there is a bug.
            if (current == null)
                throw new RuntimeException(format("Invalid null setting for guardrail on %s. This should not have"
                                                  + " happened, please contact DataStax support", what));

            // Note that this will fail on first call (as we want), as currentRaw will be null but not current
            if (current == cachedRaw)
                return;

            try
            {
                // Setting cachedAllowed first so that on a parse failure we leave everything as it previously
                // was (not that we'd expect that matter but ...).
                cachedDisallowed = current.stream()
                                          .map(parser)
                                          .collect(Collectors.toCollection(HashSet::new));
                cachedRaw = current;
            }
            catch (Exception e)
            {
                // This catch parsing errors. Hopefully, this shouldn't happen as guardrails settings should have
                // been sanitized, but ...
                // Also, we catch the exception to add a meaningful error message, but rethrow otherwise: if a
                // guardrail has been configured, it's presumably to avoid bad things to go in, so we don't wan to
                // take the risk of letting it go if there is a misconfiguration.
                throw new RuntimeException(format("Error parsing configured setting for guardrail on %s. This "
                                                  + "should not have happened, please contact DataStax support."
                                                  + "The failing setting is %s", what, current), e);
            }
        }

        /**
         * Triggers a failure if the provided value is disallowed by this guardrail.
         *
         * @param value the value to check.
         */
        public void ensureAllowed(T value)
        {
            if (!Guardrails.enabled())
                return;

            ensureUpToDate();
            if (cachedDisallowed.contains(value))
            {
                fail(format("Provided value %s is not allowed for %s (disallowed values are: %s)",
                            value, what, cachedRaw));
            }
        }

        /**
         * Triggers a failure if any of the provided values is disallowed by this guardrail.
         *
         * @param values the values to check.
         */
        public void ensureAllowed(Set<T> values)
        {
            if (!Guardrails.enabled())
                return;

            ensureUpToDate();

            Set<T> intersection = Sets.intersection(values, cachedDisallowed);
            if (!intersection.isEmpty())
                fail(format("Provided values %s are not allowed for %s (disallowed values are: %s)",
                            intersection.stream().sorted().collect(Collectors.toList()), what, cachedRaw));
        }
    }
}

