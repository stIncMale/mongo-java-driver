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

import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncWriteBinding;

/**
 * An operation which asynchronously writes to a MongoDB server.
 *
 * @param <T> the operations result type.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public interface AsyncWriteOperation<T> {

    /**
     * @return the command name of the operation, e.g. "insert", "update", "delete", "bulkWrite", etc.
     */
    String getCommandName();

    /**
     * General execute which can return anything of type T
     *
     * @param binding the binding to execute in the context of
     * @param callback the callback to be called when the operation has been executed
     */
    void executeAsync(AsyncWriteBinding binding, SingleResultCallback<T> callback);
}
