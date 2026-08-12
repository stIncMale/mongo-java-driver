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
package primer;

import static org.bson.assertions.Assertions.fail;

final class C {
    static {
        System.err.printf("%s with class loader %s is being initialized%n", C.class, C.class.getClassLoader());
//        Thread thread = App.THREAD; // does not prevent `C` or its class loader from becoming phantom reachable
        Thread thread = new Thread(null, App.SLEEPING_RUNNABLE, "sleeper", 1, false); // prevents `C` and its class loader from becoming
        // phantom reachable
        thread.start();
        System.err.printf("%s started a thread with context class loader %s%n", C.class, thread.getContextClassLoader());
    }

    private C() {
        fail();
    }
}
