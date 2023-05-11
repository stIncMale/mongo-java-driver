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

import java.util.List;
import java.util.Objects;

public final class TestPojoJava578 extends GenericPassThroughPojo<List<String>, Integer, String> {
    public TestPojoJava578() {
    }

    public List<String> getC() {
        return super.getC();
    }

    public Integer getB() {
        return super.getB();
    }

    public String getA() {
        return super.getA();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final GenericPojo<?, ?, ?> that = (GenericPojo<?, ?, ?>) o;
        return Objects.equals(getA(), that.getA()) && Objects.equals(getB(), that.getB()) && Objects.equals(getC(), that.getC());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getA(), getB(), getC());
    }
}

abstract class GenericPojo<A, B, C> {
    private A a;
    private B b;
    private C c;

    protected GenericPojo() {
    }

    public void setA(final A a) {
        this.a = a;
    }

    public A getA() {
        return a;
    }

    public void setB(final B b) {
        this.b = b;
    }

    public B getB() {
        return b;
    }

    public void setC(final C c) {
        this.c = c;
    }

    public C getC() {
        return c;
    }
}

abstract class GenericPassThroughPojo<A, B, C> extends GenericPojo<C, B, A> {
}
