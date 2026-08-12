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

import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.bson.assertions.Assertions.fail;

public final class App {
    private static final String CLASS_C_BINARY_NAME = "primer.C";
    private static final String CLASS_D_BINARY_NAME = "primer.D";
    private static final String CLASS_FILE_PATH = "/Users/valentin.kovalenko/Documents/programming/projects/mongo-java-driver/bson-record-codec/build/classes/java/main/";
    public static final Runnable SLEEPING_RUNNABLE = new Runnable() {
        @Override
        public void run() {
            String classDescription = "class " + CLASS_C_BINARY_NAME;
            try {
                Duration duration = Duration.ofSeconds(3);
                System.err.printf("%s the thread is sleeping for %s%n", classDescription, duration);
                NANOSECONDS.sleep(duration.toNanos());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                System.err.printf("%s the thread was terminated%n", classDescription);
            }
        }
    };
    public static final Thread THREAD = new Thread(SLEEPING_RUNNABLE);

    private App() {
        fail();
    }

    public static void main(final String... args) throws Exception {
        System.err.printf("%s %s by %s%n", System.getProperty("java.vm.name"), System.getProperty("java.version"), System.getProperty("java.vendor"));
        CountDownLatch phantomReachableLatch = loadAndForget();
        phantomReachableLatch.await(5,  TimeUnit.SECONDS);
    }

    private static CountDownLatch loadAndForget() throws Exception {
        ClassLoader myClassLoader = new MyClassLoader();
        String myClassLoaderDescription = myClassLoader.toString();
        Class<?> klassC = Class.forName(CLASS_C_BINARY_NAME, true, myClassLoader);
        String klassCDescription = klassC.toString();
        Class<?> klassD = Class.forName(CLASS_D_BINARY_NAME, true, myClassLoader);
        String klassDDescription = klassD.toString();
        CountDownLatch latch = new CountDownLatch(3);
        long startNanos = System.nanoTime();
        new PhantomReachableNotifier(myClassLoader, () -> {
            System.err.printf("%s has become phantom reachable in %s%n", myClassLoaderDescription, elapsed(startNanos));
            latch.countDown();
        });
        new PhantomReachableNotifier(klassC, () -> {
            System.err.printf("%s has become phantom reachable in %s%n", klassCDescription, elapsed(startNanos));
            latch.countDown();
        });
        new PhantomReachableNotifier(klassD, () -> {
            System.err.printf("%s has become phantom reachable in %s%n", klassDDescription, elapsed(startNanos));
            latch.countDown();
        });
        return latch;
    }

    private static Duration elapsed(final long startNanos) {
        return Duration.ofNanos(System.nanoTime() - startNanos);
    }

    public static final class PhantomReachableNotifier {
        private PhantomReference<Object> reference;
        private final ReferenceQueue<Object> queue = new ReferenceQueue<>();
        private final ExecutorService poller;

        public PhantomReachableNotifier(final Object referent, final Runnable notify) {
            reference = new PhantomReference<>(referent, queue);
            poller = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            poller.execute(() -> pollLoop(notify));
        }

        private void pollLoop(final Runnable notify) {
            Reference<?> queuedReference = queue.poll();
            while (queuedReference == null) {
                System.gc();
                try {
                    queuedReference = queue.remove(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            reference = null;
            notify.run();
            poller.shutdown();
        }
    }

    private static final class MyClassLoader extends ClassLoader {
        MyClassLoader() {
        }

        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            Class<?> result = findLoadedClass(name);
            if (result == null) {
                if (name.equals(CLASS_C_BINARY_NAME) || name.equals(CLASS_D_BINARY_NAME)) {
                    result = findClass(name);
                } else {
                    result = getParent().loadClass(name);
                }
            }
            if (resolve) {
                resolveClass(result);
            }
            return result;
        }

        @Override
        protected Class<?> findClass(final String name) throws ClassNotFoundException {
            if (!(name.equals(CLASS_C_BINARY_NAME) || name.equals(CLASS_D_BINARY_NAME))) {
                throw new ClassNotFoundException(name);
            }
            byte[] classFile;
            try {
                String fileName = name.replace('.', '/') + ".class";
                classFile = Files.readAllBytes(Paths.get(CLASS_FILE_PATH, fileName));
            } catch (IOException e) {
                throw new ClassNotFoundException(name, e);
            }
            return defineClass(name, classFile, 0, classFile.length);
        }
    }
}
