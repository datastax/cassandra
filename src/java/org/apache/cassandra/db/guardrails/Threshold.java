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

package org.apache.cassandra.db.guardrails;

import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import javax.annotation.Nullable;

import org.apache.cassandra.service.ClientState;

/**
 * A guardrail based on numeric threshold(s).
 *
 * <p>A {@link Threshold} guardrail defines (up to) 2 thresholds, one at which a warning is issued, and a lower one
 * at which the operation is aborted with an exception. Only one of those thresholds can be activated if desired.
 *
 * <p>This guardrail only handles guarding positive values.
 */
public abstract class Threshold extends Guardrail
{
    /**
     * A {@link Threshold} with both failure and warning thresholds disabled, so that cannot ever be triggered.
     */
    public static final Threshold NEVER_TRIGGERED = new Threshold("never_triggered", null, state -> -1L, state -> -1L, null)
    {
        @Override
        protected boolean compare(long value, long threshold)
        {
            return false;
        }

        @Override
        protected long failValue(ClientState state)
        {
            return Long.MAX_VALUE;
        }

        @Override
        protected long warnValue(ClientState state)
        {
            return Long.MAX_VALUE;
        }
    };

    protected ToLongFunction<ClientState> warnThreshold;
    protected ToLongFunction<ClientState> failThreshold;
    protected final ErrorMessageProvider messageProvider;

    /**
     * Creates a new threshold guardrail.
     *
     * @param name            the identifying name of the guardrail
     * @param reason          the optional description of the reason for guarding the operation
     * @param warnThreshold   a {@link ClientState}-based provider of the value above which a warning should be triggered.
     * @param failThreshold   a {@link ClientState}-based provider of the value above which the operation should be aborted.
     * @param messageProvider a function to generate the warning or error message if the guardrail is triggered
     */
    public Threshold(String name,
                     @Nullable String reason,
                     ToLongFunction<ClientState> warnThreshold,
                     ToLongFunction<ClientState> failThreshold,
                     ErrorMessageProvider messageProvider)
    {
        super(name, reason);
        this.warnThreshold = warnThreshold;
        this.failThreshold = failThreshold;
        this.messageProvider = messageProvider;
    }

    protected abstract boolean compare(long value, long threshold);

    protected String errMsg(boolean isWarning, String what, long value, long thresholdValue)
    {
        return messageProvider.createMessage(isWarning,
                                             what,
                                             Long.toString(value),
                                             Long.toString(thresholdValue));
    }

    private String redactedErrMsg(boolean isWarning, long value, long thresholdValue)
    {
        return errMsg(isWarning, REDACTED, value, thresholdValue);
    }

    protected abstract long failValue(ClientState state);

    protected abstract long warnValue(ClientState state);

    public boolean enabled(@Nullable ClientState state)
    {
        if (!super.enabled(state))
            return false;

        return failThreshold.applyAsLong(state) > 0 || warnThreshold.applyAsLong(state) > 0;
    }

    /**
     * Checks whether the provided value would trigger a warning or failure if passed to {@link #guard}.
     *
     * <p>This method is optional (does not have to be called) but can be used in the case where the "what"
     * argument to {@link #guard} is expensive to build to save doing so in the common case (of the guardrail
     * not being triggered).
     *
     * @param value the value to test.
     * @param state The client state, used to skip the check if the query is internal or is done by a superuser.
     *              A {@code null} value means that the check should be done regardless of the query.
     * @return {@code true} if {@code value} is above the warning or failure thresholds of this guardrail,
     * {@code false otherwise}.
     */
    public boolean triggersOn(long value, @Nullable ClientState state)
    {
        return enabled(state) && (compare(value, warnValue(state)) || compare(value, failValue(state)));
    }

    public boolean warnsOn(long value, @Nullable ClientState state)
    {
        return enabled(state) && compare(value, warnValue(state));
    }

    public boolean failsOn(long value, @Nullable ClientState state)
    {
        return enabled(state) && compare(value, failValue(state));
    }

    /**
     * Apply the guardrail to the provided value, warning or failing if appropriate.
     *
     * @param value            The value to check.
     * @param what             A string describing what {@code value} is a value of. This is used in the error message
     *                         if the guardrail is triggered. For instance, say the guardrail guards the size of column
     *                         values, then this argument must describe which column of which row is triggering the
     *                         guardrail for convenience.
     * @param containsUserData whether the {@code what} contains user data that should be redacted on external systems.
     * @param state            The client state, used to skip the check if the query is internal or is done by a superuser.
     *                         A {@code null} value means that the check should be done regardless of the query.
     */
    public void guard(long value, String what, boolean containsUserData, @Nullable ClientState state)
    {
        if (!enabled(state))
            return;

        long failValue = failValue(state);
        if (compare(value, failValue))
        {
            triggerFail(value, failValue, what, containsUserData, state);
            return;
        }

        long warnValue = warnValue(state);
        if (compare(value, warnValue))
            triggerWarn(value, warnValue, what, containsUserData);
    }

    private void triggerFail(long value, long failValue, String what, boolean containsUserData, ClientState state)
    {
        String fullMessage = errMsg(false, what, value, failValue);
        fail(fullMessage, containsUserData ? redactedErrMsg(false, value, failValue) : fullMessage, state);
    }

    private void triggerWarn(long value, long warnValue, String what, boolean containsUserData)
    {
        String fullMessage = errMsg(true, what, value, warnValue);
        warn(fullMessage, containsUserData ? redactedErrMsg(true, value, warnValue) : fullMessage);
    }

    /**
     * A function used to build the error message of a triggered {@link Threshold} guardrail.
     */
    interface ErrorMessageProvider
    {
        /**
         * Called when the guardrail is triggered to build the corresponding error message.
         *
         * @param isWarning Whether the trigger is a warning one; otherwise it is a failure one.
         * @param what      A string, provided by the call to the {@link #guard} method, describing what the guardrail
         *                  has been applied to (and that has triggered it).
         * @param value     The value that triggered the guardrail (as a string).
         * @param threshold The threshold that was passed to trigger the guardrail (as a string).
         */
        String createMessage(boolean isWarning, String what, String value, String threshold);
    }

    /**
     * Creates a new {@link GuardedCounter} guarded by this threshold guardrail.
     *
     * @param whatFct          a function called when either a warning or failure is triggered by the created counter to
     *                         describe the value. This is equivalent to the {@code what} argument of {@link #guard} but is a function to
     *                         allow the output string to be compute lazily (only if a failure/warn ends up being triggered).
     * @param containsUserData if a warning or failure is triggered by the created counter and the {@code whatFct}
     *                         is called, indicates whether the create string contains user data. This is the exact equivalent to the
     *                         similarly named argument of {@link #guard}.
     * @param clientState      the client state, used to skip the check if the query is internal or is done by a superuser.
     *                         A {@code null} value means that the check should be done regardless of the query.
     * @return the newly created guarded counter.
     */
    public GuardedCounter newCounter(Supplier<String> whatFct, boolean containsUserData, @Nullable ClientState clientState)
    {
        Threshold threshold = enabled(clientState) ? this : NEVER_TRIGGERED;
        return threshold.new GuardedCounter(whatFct, containsUserData, clientState);
    }

    /**
     * A facility for when the value to guard is built incrementally, but we want to trigger failures as soon
     * as the failure threshold is reached, but only trigger the warning on the final value (and so only if the
     * failure threshold hasn't also been reached).
     * <p>
     * Note that instances are neither thread safe nor reusable.
     */
    public class GuardedCounter
    {
        private final long warnValue;
        private final long failValue;
        private final Supplier<String> what;
        private final boolean containsUserData;

        private long accumulated;

        private GuardedCounter(Supplier<String> what, boolean containsUserData, ClientState clientState)
        {
            // We capture the warn and fail value at the time of the counter construction to ensure we use
            // stable value during the counter lifetime (and reading a final field is possibly at tad faster).
            this.warnValue = warnValue(clientState);
            this.failValue = failValue(clientState);
            this.what = what;
            this.containsUserData = containsUserData;
        }

        /**
         * The currently accumulated value of the counter.
         */
        public long get()
        {
            return accumulated;
        }

        /**
         * Add the provided increment to the counter, triggering a failure if the counter after this addition
         * crosses the failure threshold.
         *
         * @param increment the increment to add.
         */
        public void add(long increment)
        {
            accumulated += increment;
            if (accumulated > failValue)
            {
                // Pass any ClientState so GuardrailViolatedException will be thrown by Guardrail#fail
                ClientState dummyClientState = ClientState.forInternalCalls();
                triggerFail(accumulated, failValue, what.get(), containsUserData, dummyClientState);
            }
        }

        /**
         * Trigger the warn if the currently accumulated counter value crosses warning threshold and the failure
         * has not been triggered yet.
         * <p>
         * This is generally meant to be called when the guarded value is complete.
         *
         * @return {@code true} and trigger a warning if the current counter value is greater than the warning
         * threshold and less than or equal to the failure threshold, {@code false} otherwise.
         */
        public boolean checkAndTriggerWarning()
        {
            if (accumulated > warnValue && accumulated <= failValue)
            {
                triggerWarn(accumulated, warnValue, what.get(), containsUserData);
                return true;
            }
            return false;
        }
    }
}