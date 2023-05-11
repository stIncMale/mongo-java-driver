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

package org.bson.codecs.record.samples;

import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.util.Objects;

public record TestRecordWithInterfaceComponent(A a, B b, I c) {

    @BsonDiscriminator
    interface I {
    }

    public record A(String v) implements I {
    }

    public record B(boolean v) implements I {
    }

    @BsonDiscriminator(value = "C")
    public static final class C implements I {
        private int v;

        public C() {
        }

        public C setV(final int v) {
            this.v = v;
            return this;
        }

        public int getV() {
            return v;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final C c = (C) o;
            return v == c.v;
        }

        @Override
        public int hashCode() {
            return Objects.hash(v);
        }
    }
}
