/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.internal.operation;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.MongoException;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoServerException;
import com.mongodb.MongoSocketException;
import com.mongodb.assertions.Assertions;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.async.function.RetryContext;
import com.mongodb.internal.async.function.RetryPolicy;
import com.mongodb.internal.async.function.RetryPolicy.Decision.RetryAttemptInfo;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.OperationContext.ServerDeprioritization;
import com.mongodb.internal.time.ExponentialBackoff;
import com.mongodb.lang.Nullable;

import java.time.Duration;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.mongodb.MongoException.RETRYABLE_ERROR_LABEL;
import static com.mongodb.MongoException.SYSTEM_OVERLOADED_ERROR_LABEL;
import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.fail;
import static com.mongodb.internal.TimeoutContext.createMongoTimeoutException;
import static com.mongodb.internal.operation.CommandOperationHelper.DEFAULT_MAX_ADAPTIVE_RETRIES;
import static com.mongodb.internal.operation.CommandOperationHelper.NO_WRITES_PERFORMED_ERROR_LABEL;
import static com.mongodb.internal.operation.CommandOperationHelper.RETRYABLE_WRITE_ERROR_LABEL;
import static com.mongodb.internal.operation.CommandOperationHelper.addRetryableWriteErrorLabelIfNeeded;
import static com.mongodb.internal.operation.CommandOperationHelper.isRetryableException;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.isReadRetryRequirementsMet;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;

/**
 * Implements all specification retry policies.
 */
final class SpecRetryPolicy implements RetryPolicy {
    private static final int INFINITE_ATTEMPTS = Integer.MAX_VALUE;

    private final DescriptorSet descriptors;
    private int maxAttempts;
    private final ServerDeprioritization serverDeprioritization;
    private Supplier<String> commandDescriptionSupplier;

    /**
     * @param descriptors Descriptors of the included specification retry policies.
     */
    SpecRetryPolicy(
            final DescriptorSet descriptors,
            final ExplicitMaxRetries explicitMaxRetries,
            final ServerDeprioritization serverDeprioritization) {
        this.descriptors = descriptors.assertValid();
        this.maxAttempts = explicitMaxRetries.maxAttempts(descriptors);
        this.serverDeprioritization = serverDeprioritization;
        commandDescriptionSupplier = () -> null;
    }

    void onAttemptStart(final RetryContext retryContext, final OperationContext operationContext) {
        if (LOGGER.isDebugEnabled() && !retryContext.isFirstAttempt()) {
            String commandDescription = commandDescriptionSupplier.get();
            long operationId = operationContext.getId();
            Throwable prospectiveFailedResult = retryContext.getProspectiveFailedResult().orElseThrow(Assertions::fail);
            int oneBasedAttempt = retryContext.attempt() + 1;
            LOGGER.debug(commandDescription == null
                    ? format("Retrying a command within the operation with operation ID %s due to the error \"%s\". Retry attempt number: #%d",
                            operationId, prospectiveFailedResult, oneBasedAttempt)
                    : format("Retrying the command '%s' within the operation with operation ID %s due to the error \"%s\". Retry attempt number: #%d",
                            commandDescription, operationId, prospectiveFailedResult, oneBasedAttempt));
        }
    }

    /**
     * @return {@code this}.
     */
    SpecRetryPolicy onCommand(final Supplier<String> commandDescriptionSupplier) {
        this.commandDescriptionSupplier = commandDescriptionSupplier;
        return this;
    }

    /**
     * The information gathered via this method is reset after each invocation of {@link #onAttemptFailure(RetryContext, Throwable)}.
     *
     * @param remainingWriteRequirementsMet This argument, combined with the information passed to
     * {@link DescriptorSet#DescriptorSet(boolean)} and {@link DescriptorSet#includeWrite()}, specifies whether the write retry requirements are met.
     * <p>
     * Specifying {@code false}, or not calling this method at all, does not completely prevent retrying,
     * but affects logging and which failed results may be eligible for retry.
     * For example, {@link MongoConnectionPoolClearedException} may be eligible for retry regardless of this flag.
     * @return {@code this}.
     */
    SpecRetryPolicy onWriteRetryRequirements(final boolean remainingWriteRequirementsMet, final ConnectionDescription connectionDescription) {
        return descriptors.write().map(state -> {
            state.onRequirements(remainingWriteRequirementsMet, connectionDescription);
            return this;
        }).orElseThrow(() -> fail());
    }

    @Override
    public Decision onAttemptFailure(final RetryContext retryContext, final Throwable maybeInternalAttemptFailedResult) {
        Throwable attemptFailedResult = stripResourceSupplierInternalException(maybeInternalAttemptFailedResult);
        serverDeprioritization.onAttemptFailure(attemptFailedResult);
        int attempt = retryContext.attempt();
        assertTrue(attempt < INFINITE_ATTEMPTS);
        assertTrue(attempt < maxAttempts);
        boolean retryableError = false;
        retryableError |= descriptors.write().map(state ->
            decideRetryableAndAddRetryableWriteErrorLabelIfNeeded(state, attemptFailedResult)).orElse(false);
        retryableError |= descriptors.read().map(state ->
            decideRetryable(state, attemptFailedResult)).orElse(false);
        retryableError |= descriptors.overload().map(state ->
            decideRetryableAndUpdateMaxAttempts(state, attemptFailedResult)).orElse(false);
        boolean maxAttemptsReached = attempt >= maxAttempts - 1;
        if (descriptors.isRetrySettingEffectivelyEnabled() && !retryableError) {
            logUnableToRetryError(attemptFailedResult);
        }
        boolean retry = retryableError && !maxAttemptsReached;
        Decision decision = new Decision(
                decideProspectiveFailedResult(retryContext.getProspectiveFailedResult().orElse(null), maybeInternalAttemptFailedResult),
                retry ? new RetryAttemptInfo(calculateOverloadBackoff(attemptFailedResult, attempt + 1)) : null);
        descriptors.write().ifPresent(DescriptorSet.State.Write::resetRequirementsInfo);
        return decision;
    }

    private static Throwable stripResourceSupplierInternalException(final Throwable maybeInternal) {
        Throwable external;
        if (maybeInternal instanceof OperationHelper.ResourceSupplierInternalException) {
            external = maybeInternal.getCause();
        } else {
            external = maybeInternal;
        }
        return external;
    }

    /**
     * Returns {@code true} iff another attempt must be executed provided that {@link #maxAttempts} has not been reached;
     * in this case, also adds the {@value CommandOperationHelper#RETRYABLE_WRITE_ERROR_LABEL} label if needed.
     */
    private boolean decideRetryableAndAddRetryableWriteErrorLabelIfNeeded(final DescriptorSet.State.Write state, final Throwable exception) {
        assertFalse(exception instanceof OperationHelper.ResourceSupplierInternalException);
        if (!(exception instanceof MongoException)) {
            return false;
        }
        MongoException mongoException = (MongoException) exception;
        boolean connectionPoolClearedException = mongoException instanceof MongoConnectionPoolClearedException;
        if (connectionPoolClearedException && descriptors.isRetrySettingEffectivelyEnabled()) {
            // We would have retried regardless of the settings,
            // but we add `RETRYABLE_WRITE_ERROR_LABEL` only if retries are enabled via settings.
            mongoException.addLabel(RETRYABLE_WRITE_ERROR_LABEL);
        }
        boolean retryRegardlessOfRequirementsHavingBeenMet = connectionPoolClearedException || isRetryableMongoSecurityException(mongoException);
        boolean retry;
        if (state.isRequirementsMet()) {
            addRetryableWriteErrorLabelIfNeeded(mongoException, state.getMaxWireVersion());
            retry = mongoException.hasErrorLabel(RETRYABLE_WRITE_ERROR_LABEL);
        } else {
            retry = retryRegardlessOfRequirementsHavingBeenMet;
        }
        return retry;
    }

    private static boolean isRetryableMongoSecurityException(final MongoException exception) {
        return exception instanceof MongoSecurityException
                && exception.getCause() != null && isRetryableException(exception.getCause());
    }

    /**
     * Returns {@code true} iff another attempt must be executed provided that {@link #maxAttempts} has not been reached.
     */
    private static boolean decideRetryable(final DescriptorSet.State.Read state, final Throwable exception) {
        assertFalse(exception instanceof OperationHelper.ResourceSupplierInternalException);
        if (!state.isRequirementsMet() || !(exception instanceof MongoException)) {
            return false;
        }
        MongoException mongoException = (MongoException) exception;
        return isRetryableMongoSecurityException(mongoException) || isRetryableException(mongoException);
    }

    /**
     * Returns {@code true} iff another attempt must be executed provided that {@link #maxAttempts} has not been reached.
     */
    private boolean decideRetryableAndUpdateMaxAttempts(final DescriptorSet.State.Overload state, final Throwable exception) {
        boolean hasRetryableErrorLabel;
        boolean hasSystemOverloadErrorLabel;
        if (exception instanceof MongoException) {
            MongoException mongoException = (MongoException) exception;
            hasRetryableErrorLabel = mongoException.hasErrorLabel(RETRYABLE_ERROR_LABEL);
            hasSystemOverloadErrorLabel = mongoException.hasErrorLabel(SYSTEM_OVERLOADED_ERROR_LABEL);
        } else {
            hasRetryableErrorLabel = false;
            hasSystemOverloadErrorLabel = false;
        }
        if (hasSystemOverloadErrorLabel && descriptors.isRetrySettingEffectivelyEnabled()) {
            maxAttempts = state.getMaxAttempts();
        }
        return state.isRequirementsMet() && hasRetryableErrorLabel && hasSystemOverloadErrorLabel;
    }

    private void logUnableToRetryError(final Throwable exception) {
        assertFalse(exception instanceof OperationHelper.ResourceSupplierInternalException);
        if (LOGGER.isDebugEnabled()) {
            String commandDescription = commandDescriptionSupplier.get();
            LOGGER.debug(commandDescription == null
                    ? format("Unable to retry a command due to the error \"%s\"", exception)
                    : format("Unable to retry the command '%s' due to the error \"%s\"", commandDescription, exception));
        }
    }

    private Throwable decideProspectiveFailedResult(
            @Nullable final Throwable currentProspectiveFailedResult, final Throwable mostRecentAttemptFailedResult) {
        Throwable newProspectiveFailedResult;
        if (descriptors.write().isPresent()) {
            newProspectiveFailedResult = decideWriteProspectiveFailedResult(currentProspectiveFailedResult, mostRecentAttemptFailedResult);
        } else if (descriptors.read().isPresent()) {
            newProspectiveFailedResult = decideReadProspectiveFailedResult(currentProspectiveFailedResult, mostRecentAttemptFailedResult);
        } else {
            throw fail(toString());
        }
        if (mostRecentAttemptFailedResult instanceof MongoOperationTimeoutException) {
            newProspectiveFailedResult = createMongoTimeoutException(newProspectiveFailedResult);
        }
        return newProspectiveFailedResult;
    }

    private static Throwable decideWriteProspectiveFailedResult(
            @Nullable final Throwable currentProspectiveFailedResult, final Throwable maybeInternalMostRecentAttemptFailedResult) {
        if (currentProspectiveFailedResult == null) {
            return stripResourceSupplierInternalException(maybeInternalMostRecentAttemptFailedResult);
        } else if (maybeInternalMostRecentAttemptFailedResult instanceof OperationHelper.ResourceSupplierInternalException
                || (maybeInternalMostRecentAttemptFailedResult instanceof MongoException
                        && ((MongoException) maybeInternalMostRecentAttemptFailedResult).hasErrorLabel(NO_WRITES_PERFORMED_ERROR_LABEL))) {
            return currentProspectiveFailedResult;
        } else {
            return maybeInternalMostRecentAttemptFailedResult;
        }
    }

    private static Throwable decideReadProspectiveFailedResult(
            @Nullable final Throwable currentProspectiveFailedResult, final Throwable mostRecentAttemptFailedResult) {
        assertFalse(mostRecentAttemptFailedResult instanceof OperationHelper.ResourceSupplierInternalException);
        if (currentProspectiveFailedResult == null
                || mostRecentAttemptFailedResult instanceof MongoSocketException
                || mostRecentAttemptFailedResult instanceof MongoServerException) {
            return mostRecentAttemptFailedResult;
        } else {
            return currentProspectiveFailedResult;
        }
    }

    private static Duration calculateOverloadBackoff(final Throwable attemptFailedResult, final int immediateNextAttempt) {
        assertFalse(attemptFailedResult instanceof OperationHelper.ResourceSupplierInternalException);
        if (attemptFailedResult instanceof MongoException
                && ((MongoException) attemptFailedResult).hasErrorLabel(SYSTEM_OVERLOADED_ERROR_LABEL)) {
            return ExponentialBackoff.calculateOverloadBackoff(immediateNextAttempt);
        }
        return Duration.ZERO;
    }

    private static int maxAttempts(final int maxRetries) {
        assertTrue(maxRetries < INFINITE_ATTEMPTS - 1);
        return maxRetries + 1;
    }

    @Override
    public String toString() {
        return "SpecRetryPolicy{"
                + "descriptors=" + descriptors
                + ", maxAttempts=" + maxAttempts
                + ", serverDeprioritization=" + serverDeprioritization
                + ", commandDescription=" + commandDescriptionSupplier.get()
                + '}';
    }

    static final class DescriptorSet {
        private static final EnumMap<Descriptor, EnumSet<Descriptor>> CONFLICTS;

        private final boolean effectiveRetrySetting;
        private final EnumMap<Descriptor, State> descriptors;

        static {
            CONFLICTS = new EnumMap<>(Descriptor.class);
            CONFLICTS.put(Descriptor.WRITE, EnumSet.of(Descriptor.READ));
            CONFLICTS.put(Descriptor.READ, EnumSet.of(Descriptor.WRITE));
        }

        /**
         * @param effectiveRetrySetting See {@link MongoClientSettings#getRetryWrites()}, {@link MongoClientSettings#getRetryReads()}.
         * Note that for some commands, like {@code commitTransaction}/{@code abortTransaction},
         * retries are deemed to be enabled regardless of the settings; this argument must be {@code true} for them.
         */
        DescriptorSet(final boolean effectiveRetrySetting) {
            this.effectiveRetrySetting = effectiveRetrySetting;
            this.descriptors = new EnumMap<>(Descriptor.class);
        }

        private DescriptorSet assertValid() {
            assertFalse(descriptors.isEmpty());
            assertNoConflicts(descriptors.keySet());
            return this;
        }

        private static void assertNoConflicts(final Set<Descriptor> descriptors) {
            for (Descriptor descriptor : descriptors) {
                Set<Descriptor> conflictingDescriptors = CONFLICTS.get(descriptor);
                if (conflictingDescriptors != null) {
                    for (Descriptor conflictingDescriptor : conflictingDescriptors) {
                        assertFalse(descriptors.contains(conflictingDescriptor));
                    }
                }
            }
        }

        private boolean isRetrySettingEffectivelyEnabled() {
            return effectiveRetrySetting;
        }

        DescriptorSet includeWrite() {
            include(Descriptor.WRITE, new State.Write(effectiveRetrySetting));
            return this;
        }

        DescriptorSet includeRead(final OperationContext operationContext) {
            include(Descriptor.READ, new State.Read(effectiveRetrySetting, operationContext));
            return this;
        }

        DescriptorSet includeOverload(@Nullable final Integer maxAdaptiveRetriesSetting) {
            include(Descriptor.OVERLOAD, new State.Overload(effectiveRetrySetting, maxAdaptiveRetriesSetting));
            return this;
        }

        private void include(final Descriptor descriptor, final State state) {
            State previous = descriptors.put(descriptor, state);
            assertNull(previous);
        }

        private int getInitialMaxAttempts() {
            int maxRetries;
            if (descriptors.containsKey(Descriptor.WRITE)) {
                maxRetries = Descriptor.WRITE.maxRetries;
            } else if (descriptors.containsKey(Descriptor.READ)) {
                maxRetries = Descriptor.READ.maxRetries;
            } else if (descriptors.containsKey(Descriptor.OVERLOAD)) {
                maxRetries = overload().map(state -> state.getMaxAttempts()).orElse(Descriptor.OVERLOAD.maxRetries);
            } else {
                throw fail(toString());
            }
            return maxAttempts(maxRetries);
        }

        private Optional<State.Write> write() {
            return Optional.ofNullable((State.Write) descriptors.get(Descriptor.WRITE));
        }

        private Optional<State.Read> read() {
            return Optional.ofNullable((State.Read) descriptors.get(Descriptor.READ));
        }

        private Optional<State.Overload> overload() {
            return Optional.ofNullable((State.Overload) descriptors.get(Descriptor.OVERLOAD));
        }

        @Override
        public String toString() {
            return "DescriptorSet{"
                    + "effectiveRetrySetting=" + effectiveRetrySetting
                    + ", descriptors=" + descriptors
                    + '}';
        }

        private enum Descriptor {
            /**
             * See
             * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.md">Retryable Writes</a>,
             * which specifies the write retry policy.
             */
            WRITE(1),
            /**
             * See
             * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-reads/retryable-reads.md">Retryable Reads</a>,
             * which specifies the read retry policy.
             */
            READ(1),
            /**
             * See
             * <a href="https://github.com/mongodb/specifications/blob/master/source/client-backpressure/client-backpressure.md">Client Backpressure</a>,
             * which specifies the overload retry policy.
             */
            OVERLOAD(DEFAULT_MAX_ADAPTIVE_RETRIES);

            private final int maxRetries;

            Descriptor(final int maxRetries) {
                this.maxRetries = maxRetries;
            }
        }

        private abstract static class State {
            private State() {
            }

            static final class Write extends State {
                private final boolean requirementsMaybeMet;
                @Nullable
                private Boolean requirementsMet;
                @Nullable
                private Integer maxWireVersion;

                Write(final boolean effectiveRetryWritesSetting) {
                    this.requirementsMaybeMet = effectiveRetryWritesSetting;
                }

                /**
                 * @see #isRequirementsMet()
                 * @see #resetRequirementsInfo()
                 * @see SpecRetryPolicy#onWriteRetryRequirements(boolean, ConnectionDescription)
                 */
                void onRequirements(
                        final boolean remainingRequirementsMet,
                        final ConnectionDescription connectionDescription) {
                    assertNull(requirementsMet);
                    requirementsMet = requirementsMaybeMet && remainingRequirementsMet;
                    maxWireVersion = connectionDescription.getMaxWireVersion();
                }

                /**
                 * @see #onRequirements(boolean, ConnectionDescription)
                 * @see SpecRetryPolicy#onWriteRetryRequirements(boolean, ConnectionDescription)
                 */
                void resetRequirementsInfo() {
                    maxWireVersion = null;
                    requirementsMet = null;
                }

                /**
                 * The requirements in question include settings requirements, server requirements, command requirements, etc.,
                 * but exclude error requirements (including operation timeout requirements), {@link #maxAttempts} requirements.
                 *
                 * @see #onRequirements(boolean, ConnectionDescription)
                 */
                boolean isRequirementsMet() {
                    return TRUE.equals(requirementsMet);
                }

                /**
                 * May be called only if {@link #isRequirementsMet()}.
                 */
                int getMaxWireVersion() {
                    return assertNotNull(maxWireVersion);
                }

                @Override
                public String toString() {
                    return "Write{"
                            + "requirementsMaybeMet=" + requirementsMaybeMet
                            + ", requirementsMet=" + requirementsMet
                            + ", maxWireVersion=" + maxWireVersion
                            + '}';
                }
            }

            static final class Read extends State {
                private final boolean requirementsMet;

                Read(final boolean effectiveRetryReadsSetting, final OperationContext operationContext) {
                    requirementsMet = isReadRetryRequirementsMet(effectiveRetryReadsSetting, operationContext);
                }

                /**
                 * The requirements in question include settings requirements, server requirements, command requirements, etc.,
                 * but exclude error requirements (including operation timeout requirements), {@link #maxAttempts} requirements.
                 */
                boolean isRequirementsMet() {
                    return requirementsMet;
                }

                @Override
                public String toString() {
                    return "Read{"
                            + "requirementsMet=" + requirementsMet
                            + '}';
                }
            }

            static final class Overload extends State {
                private final boolean requirementsMet;
                @Nullable
                private final Integer maxAdaptiveRetriesSetting;

                Overload(
                        final boolean effectiveRetrySetting,
                        @Nullable
                        final Integer maxAdaptiveRetriesSetting) {
                    requirementsMet = effectiveRetrySetting;
                    this.maxAdaptiveRetriesSetting = maxAdaptiveRetriesSetting;
                }

                /**
                 * The requirements in question include settings requirements, server requirements, command requirements, etc.,
                 * but exclude error requirements (including operation timeout requirements), {@link #maxAttempts} requirements.
                 */
                boolean isRequirementsMet() {
                    return requirementsMet;
                }

                int getMaxAttempts() {
                    return maxAttempts(maxAdaptiveRetriesSetting == null
                            ? DescriptorSet.Descriptor.OVERLOAD.maxRetries
                            : maxAdaptiveRetriesSetting);
                }

                @Override
                public String toString() {
                    return "OverloadState{"
                            + "requirementsMet=" + requirementsMet
                            + ", maxAdaptiveRetriesSetting=" + maxAdaptiveRetriesSetting
                            + '}';
                }
            }
        }
    }

    enum ExplicitMaxRetries {
        NO_RETRIES_LIMIT,
        /**
         * See {@link DescriptorSet}.
         */
        RETRIES_LIMITED_BY_DESCRIPTORS;

        private int maxAttempts(final DescriptorSet descriptors) {
            switch (this) {
                case NO_RETRIES_LIMIT: {
                    return INFINITE_ATTEMPTS;
                }
                case RETRIES_LIMITED_BY_DESCRIPTORS: {
                    return descriptors.getInitialMaxAttempts();
                }
                default: {
                    throw fail(this.toString());
                }
            }
        }
    }
}
