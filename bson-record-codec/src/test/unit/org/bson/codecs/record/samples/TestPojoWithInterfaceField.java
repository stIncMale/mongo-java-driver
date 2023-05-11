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

public final class TestPojoWithInterfaceField {
    private A a;
    private B b;
    private I c;

    public TestPojoWithInterfaceField() {
    }

    public TestPojoWithInterfaceField setA(final A a) {
        this.a = a;
        return this;
    }

    public A getA() {
        return a;
    }

    public TestPojoWithInterfaceField setB(final B b) {
        this.b = b;
        return this;
    }

    public B getB() {
        return b;
    }

    public TestPojoWithInterfaceField setC(final I c) {
        this.c = c;
        return this;
    }

    public I getC() {
        return c;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TestPojoWithInterfaceField that = (TestPojoWithInterfaceField) o;
        return Objects.equals(a, that.a) && Objects.equals(b, that.b) && Objects.equals(c, that.c);
    }

    @Override
    public int hashCode() {
        return Objects.hash(a, b, c);
    }

    @BsonDiscriminator
    interface I {
    }

    public static final class A implements I {
        private String v;

        public A() {
        }

        public A setV(final String v) {
            this.v = v;
            return this;
        }

        public String getV() {
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
            final A a = (A) o;
            return v == a.v;
        }

        @Override
        public int hashCode() {
            return Objects.hash(v);
        }
    }

    public static final class B implements I {
        private boolean v;

        public B() {
        }

        public B setV(final boolean v) {
            this.v = v;
            return this;
        }

        public boolean getV() {
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
            final B b = (B) o;
            return v == b.v;
        }

        @Override
        public int hashCode() {
            return Objects.hash(v);
        }
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
