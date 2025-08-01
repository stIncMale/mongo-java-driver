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

import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.inject.Provider;

class TestServerMonitor implements ServerMonitor {
    private final Provider<SdamServerDescriptionManager> sdamProvider;

    TestServerMonitor(final Provider<SdamServerDescriptionManager> sdamProvider) {
        this.sdamProvider = sdamProvider;
    }

    @Override
    public void start() {
    }

    @Override
    public void connect() {
    }

    @Override
    public void close() {
    }

    @Override
    public void cancelCurrentCheck() {
    }

    public void updateServerDescription(final ServerDescription serverDescription) {
        sdamProvider.get().monitorUpdate(serverDescription);
    }
}
