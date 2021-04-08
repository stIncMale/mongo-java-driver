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

package com.mongodb.internal.connection;

import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.lang.Nullable;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

/**
 * An instance of an implementation must be created in the {@linkplain #invalidate() "paused"} state.
 */
interface ConnectionPool extends Closeable {

    /**
     * Start background tasks, e.g., creating connections in the background. This method does not change the state of the pool,
     * but the actions performed by background tasks may depend on the state.
     */
    void start();

    /**
     * Is equivalent to {@link #get(long, TimeUnit)} called with {@link ConnectionPoolSettings#getMaxWaitTime(TimeUnit)}.
     */
    InternalConnection get() throws MongoConnectionPoolClearedException;

    /**
     * @param timeout See {@link com.mongodb.internal.Timeout#startNow(long, TimeUnit)}.
     * @throws MongoConnectionPoolClearedException If detects that the pool is "paused".
     */
    InternalConnection get(long timeout, TimeUnit timeUnit) throws MongoConnectionPoolClearedException;

    /**
     * Completes the {@code callback} with a {@link MongoConnectionPoolClearedException} if detects that the pool is "paused".
     */
    void getAsync(SingleResultCallback<InternalConnection> callback);

    /**
     * Mark the pool as "paused", unblock all threads waiting in {@code get...} methods, unless they are blocked
     * doing an IO operation, lazily clear all connections managed by the pool (this is done via {@code get...} and
     * {@link InternalConnection#close() check in} methods). In the "paused" state, connections can be created neither in the background
     * nor via {@code get...} methods.
     *
     * @see #ready()
     */
    void invalidate(@Nullable Throwable cause);

    /**
     * Is equivalent to {@link #invalidate(Throwable)} called with {@code null}.
     */
    void invalidate();

    /**
     * Mark the pool as "ready", allowing connections to be created in the background and via {@code get...} methods.
     *
     * @see #invalidate(Throwable)
     */
    void ready();

    /**
     * Mark the pool as "closed", release the underlying resources and render the pool unusable.
     */
    void close();

    int getGeneration();
}
