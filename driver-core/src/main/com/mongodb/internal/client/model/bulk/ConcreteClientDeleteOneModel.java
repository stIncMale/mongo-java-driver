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
package com.mongodb.internal.client.model.bulk;

import com.mongodb.client.model.bulk.ClientDeleteOneOptions;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public final class ConcreteClientDeleteOneModel extends AbstractClientDeleteModel<ConcreteClientDeleteOneOptions> implements ClientWriteModel {
    public ConcreteClientDeleteOneModel(final Bson filter, @Nullable final ClientDeleteOneOptions options) {
        super(filter, options == null ? ConcreteClientDeleteOneOptions.MUTABLE_EMPTY : (ConcreteClientDeleteOneOptions) options);
    }

    @Override
    public String toString() {
        return "ClientDeleteOneModel{"
                + "filter=" + getFilter()
                + ", options=" + getOptions()
                + '}';
    }
}
