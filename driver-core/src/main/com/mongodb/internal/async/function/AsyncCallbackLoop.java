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
package com.mongodb.internal.async.function;

import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.lang.Nullable;

import java.util.function.Supplier;

/**
 * A decorator that implements automatic repeating of an {@link AsyncCallbackRunnable}.
 * {@link AsyncCallbackLoop} may execute the original asynchronous function multiple times sequentially,
 * while guaranteeing that the callback passed to {@link #run(SingleResultCallback)} is completed at most once.
 * This class emulates the <a href="https://docs.oracle.com/javase/specs/jls/se8/html/jls-14.html#jls-14.12">{@code while(true)}</a>
 * statement.
 * <p>
 * The original function may additionally observe or control looping via {@link LoopState}.
 * Looping continues until either of the following happens:
 * <ul>
 *     <li>the original function fails as specified by {@link AsyncCallbackFunction};</li>
 *     <li>the original function calls {@link LoopState#breakAndCompleteIf(Supplier, SingleResultCallback)}.</li>
 * </ul>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@NotThreadSafe
public final class AsyncCallbackLoop implements AsyncCallbackRunnable {
    private final LoopState state;
    private final AsyncCallbackRunnable body;
    private final ThreadLocal<IterationResult> iterationResult;

    /**
     * @param state The {@link LoopState} to be deemed as initial for the purpose of the new {@link AsyncCallbackLoop}.
     * @param body The body of the loop.
     */
    public AsyncCallbackLoop(final LoopState state, final AsyncCallbackRunnable body) {
        this.body = body;
        this.state = state;
        iterationResult = ThreadLocal.withInitial(() -> IterationResult.UNKNOWN);
    }

    @Override
    public void run(final SingleResultCallback<Void> callback) {
        run(true, callback);
    }

    /**
     * Initiates a new iteration of the loop by invoking
     * {@link #body}{@code .}{@link AsyncCallbackRunnable#run(SingleResultCallback) run}.
     * The initiated iteration may be executed either synchronously or asynchronously with the method that initiated it:
     * <ul>
     *     <li>synchronous execution—completion of the initiated iteration is guaranteed to happen-before the method completion;
     *          <ul>
     *              <li>Note that the formulations
     *                  <ol>
     *                      <li>"completion of the initiated iteration is guaranteed to happen-before the method completion"</li>
     *                      <li>"completion of the initiated iteration happens-before the method completion"</li>
     *                  </ol>
     *                  are different: the former is about the program while the latter is about the execution, and follows from the former.
     *                  For us the former is useful.
     *              </li>
     *          </ul>
     *     </li>
     *     <li>asynchronous execution—the aforementioned guarantee does not exist.
     *          <ul>
     *              <li>Note that the formulations
     *                  <ol>
     *                      <li>"the aforementioned guarantee does not exist"</li>
     *                      <li>"the aforementioned relation does not exist"</li>
     *                  </ol>
     *                  are different: the former is about the program while the latter is about the execution, and follows from the former.
     *                  For us the former is useful.
     *              </li>
     *          </ul>
     *     </li>
     * </ul>
     *
     * <p>If another iteration is needed, it is initiated from the callback passed to
     * {@link #body}{@code .}{@link AsyncCallbackRunnable#run(SingleResultCallback) run}
     * by invoking {@link #run(boolean, SingleResultCallback)}.
     * Completing the initiated iteration is {@linkplain SingleResultCallback#onResult(Object, Throwable) invoking} the callback.
     * Thus, it is guaranteed that all iterations are executed sequentially with each other
     * (that is, completion of one iteration happens-before initiation of the next one)
     * regardless of them being executed synchronously or asynchronously with the method that initiated them.
     *
     * <p>Iterations are executed using trampolining, which results in iterative execution rather than a recursive one,
     * and ensures stack usage does not increase with the number of iterations.
     *
     * @return {@code true} iff another iteration must be initiated in the current thread.
     */
    boolean run(final boolean firstIteration, final SingleResultCallback<Void> afterLoopCallback) {
        try {
            if (!firstIteration) {
                iterationResult.set(IterationResult.NEEDED);
            }
            body.run((r, t) -> { // this callback can be trivially reused, making the iteration garbage-free
                if (completeIfNeeded(afterLoopCallback, r, t)) {
                    // we have just completed the last iteration, the loop is complete
                    iterationResult.remove();
                    return;
                }
                if (iterationResult.get().equals(IterationResult.NEEDED)) {
                    // Bounce if we are trampolining and the iteration was completed by the same thread that initiated it;
                    // otherwise proceed to initiate trampolining.
                    iterationResult.set(IterationResult.INITIATE_ANOTHER_IN_THE_SAME_THREAD);
                    return;
                }
                try {
                    // initiate trampolining
                    boolean continueTrampolining;
                    do {
                        continueTrampolining = run(false, afterLoopCallback);
                    } while (continueTrampolining);
                } finally {
                    iterationResult.remove();
                }
            });
            return iterationResult.get().equals(IterationResult.INITIATE_ANOTHER_IN_THE_SAME_THREAD);
        } finally {
            iterationResult.remove();
        }
    }

    /**
     * @return {@code true} iff the {@code afterLoopCallback} was
     * {@linkplain SingleResultCallback#onResult(Object, Throwable) completed}.
     */
    private boolean completeIfNeeded(final SingleResultCallback<Void> afterLoopCallback,
            @Nullable final Void result, @Nullable final Throwable t) {
        if (t != null) {
            afterLoopCallback.onResult(null, t);
            return true;
        } else {
            boolean anotherIterationNeeded;
            try {
                anotherIterationNeeded = state.advance();
            } catch (Throwable e) {
                afterLoopCallback.onResult(null, e);
                return true;
            }
            if (anotherIterationNeeded) {
                return false;
            } else {
                afterLoopCallback.onResult(result, null);
                return true;
            }
        }
    }

    private enum IterationResult {
        UNKNOWN,
        NEEDED,
        INITIATE_ANOTHER_IN_THE_SAME_THREAD
    }
}
