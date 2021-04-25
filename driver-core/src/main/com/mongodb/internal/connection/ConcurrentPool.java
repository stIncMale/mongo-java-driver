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

import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ConnectionId;
import com.mongodb.internal.connection.ConcurrentLinkedDeque.RemovalReportingIterator;
import com.mongodb.lang.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;

/**
 * A concurrent pool implementation.
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public class ConcurrentPool<T> implements Pool<T> {
    private static final String POOL_CLOSED_MESSAGE = "The pool is closed";

    private final int maxSize;
    private final ItemFactory<T> itemFactory;

    private final ConcurrentLinkedDeque<T> available = new ConcurrentLinkedDeque<T>();
    private final StateAndPermits stateAndPermits;

    public enum Prune {
        /**
         * Prune this element
         */
        YES,
        /**
         * Don't prone this element
         */
        NO,
        /**
         * Don't prune this element and stop attempting to prune additional elements
         */
        STOP
    }
    /**
     * Factory for creating and closing pooled items.
     *
     * @param <T>
     */
    public interface ItemFactory<T> {
        T create();

        void close(T t);

        Prune shouldPrune(T t);
    }

    /**
     * Initializes a new pool of objects.
     *
     * @param maxSize     max to hold to at any given time. if < 0 then no limit
     * @param itemFactory factory used to create and close items in the pool
     */
    public ConcurrentPool(final int maxSize, final ItemFactory<T> itemFactory) {
        this.maxSize = maxSize;
        this.itemFactory = itemFactory;
        stateAndPermits = new StateAndPermits(maxSize);
    }

    /**
     * Return an instance of T to the pool.  This method simply calls {@code release(t, false)}
     * Must not throw {@link Exception}s.
     *
     * @param t item to return to the pool
     */
    @Override
    public void release(final T t) {
        release(t, false);
    }

    /**
     * call done when you are done with an object from the pool if there is room and the object is ok will get added
     * Must not throw {@link Exception}s.
     *
     * @param t     item to return to the pool
     * @param prune true if the item should be closed, false if it should be put back in the pool
     */
    @Override
    public void release(final T t, final boolean prune) {
        if (t == null) {
            throw new IllegalArgumentException("Can not return a null item to the pool");
        }

        if (stateAndPermits.closed()) {
            close(t);
            return;
        }

        rrelease(t);
        if (prune) {
            close(t);
        } else {
            available.addLast(t);
        }

        stateAndPermits.releasePermit();
    }

    /**
     * Gets an object from the pool. This method will block until a permit is available or the pool is {@linkplain #close() closed}.
     *
     * @return An object from the pool.
     * @throws MongoTimeoutException See {@link #get(long, TimeUnit)}.
     */
    @Override
    public T get() {
        return get(-1, TimeUnit.MILLISECONDS);
    }

    /**
     * Gets an object from the pool - will block if none are available
     *
     * @param timeout  See {@link com.mongodb.internal.Timeout#startNow(long, TimeUnit)}.
     * @param timeUnit the time unit of the timeout
     * @return An object from the pool, or null if can't get one in the given waitTime
     * @throws MongoTimeoutException if the timeout has been exceeded or the pool was {@linkplain #close() closed}.
     */
    @Override
    public T get(final long timeout, final TimeUnit timeUnit) {
        stateAndPermits.throwIfClosed();
        stateAndPermits.throwIfPaused();

        if (!stateAndPermits.acquirePermitFair(timeout, timeUnit)) {
            throw new MongoTimeoutException(String.format("Timeout waiting for a pooled item after %d %s", timeout, timeUnit));
        }

        T t = available.pollLast();
        if (t == null) {
            t = createNewAndReleasePermitIfFailure(noInit -> {});
        }
        gget(t, "get");
        return t;
    }

    /**
     * This method is similar to {@link #get(long, TimeUnit)} with 0 timeout.
     * The difference is that it never creates a new element,
     * returns {@code null} instead of throwing {@link MongoTimeoutException} and may not be fair.
     */
    @Nullable
    T getImmediateUnfair() {
        stateAndPermits.throwIfClosed();
        stateAndPermits.throwIfPaused();
        T element = null;
        if (stateAndPermits.acquirePermitImmediateUnfair()) {
            element = available.pollLast();
            if (element == null) {
                stateAndPermits.releasePermit();
            } else {
                gget(element, "getImmediateUnfair");
            }
        }
        return element;
    }

    public void prune() {
        for (RemovalReportingIterator<T> iter = available.iterator(); iter.hasNext();) {
            T cur = iter.next();
            Prune shouldPrune = itemFactory.shouldPrune(cur);

            if (shouldPrune == Prune.STOP) {
                break;
            }

            if (shouldPrune == Prune.YES) {
                boolean removed = iter.reportingRemove();
                if (removed) {
                    close(cur);
                }
            }
        }
    }

    /**
     * Try to populate this pool with items so that {@link #getCount()} is not smaller than {@code minSize}.
     * The {@code postCreate} action throwing a exception causes this method to stop and re-throw that exception.
     *
     * @param initialize An action applied to non-{@code null} new items.
     *                   If this action throws an {@link Exception}, it must release resources associated with the item.
     */
    public void ensureMinSize(final int minSize, final Consumer<T> initialize) {
        stateAndPermits.throwIfClosed();
        stateAndPermits.throwIfPaused();
        while (getCount() < minSize) {
            if (!stateAndPermits.acquirePermitFair(0, TimeUnit.MILLISECONDS)) {
                break;
            }
            T newItem = createNewAndReleasePermitIfFailure(initialize);
            gget(newItem, "get ensureMinSize");
            release(newItem);
        }
    }

    /**
     * @param initialize See {@link #ensureMinSize(int, Consumer)}.
     */
    private T createNewAndReleasePermitIfFailure(final Consumer<T> initialize) {
        try {
            T newMember = itemFactory.create();
            if (newMember == null) {
                throw new MongoInternalException("The factory for the pool created a null item");
            }
            initialize.accept(newMember);
            return newMember;
        } catch (RuntimeException e) {
            stateAndPermits.releasePermit();
            throw e;
        }
    }

    /**
     * Is package-access for the purpose of testing and must not be used for any other purpose outside of this class.
     *
     * @param timeout See {@link com.mongodb.internal.Timeout#startNow(long, TimeUnit)}.
     */
    boolean acquirePermit(final long timeout, final TimeUnit timeUnit) {
        return stateAndPermits.acquirePermitFair(timeout, timeUnit);
    }

    /**
     * Clears the pool of all objects.
     * Must not throw {@link Exception}s.
     */
    @Override
    public void close() {
        if (stateAndPermits.close()) {
            Iterator<T> iter = available.iterator();
            while (iter.hasNext()) {
                T t = iter.next();
                close(t);
                iter.remove();
            }
        }
    }

    public int getMaxSize() {
        return maxSize;
    }

    public int getInUseCount() {
        return maxSize - stateAndPermits.permits();
    }

    public int getAvailableCount() {
        return available.size();
    }

    public int getCount() {
        return getInUseCount() + getAvailableCount();
    }

    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("pool: ")
           .append(" maxSize: ").append(maxSize)
           .append(" availableCount ").append(getAvailableCount())
           .append(" inUseCount ").append(getInUseCount());
        return buf.toString();
    }

    /**
     * Must not throw {@link Exception}s, so swallow exceptions from {@link ItemFactory#close(Object)}.
     */
    private void close(final T t) {
        try {
            itemFactory.close(t);
        } catch (RuntimeException e) {
            // ItemFactory.close() really should not throw
        }
    }

    void ready() {
        stateAndPermits.ready();
    }

    void pause(final Supplier<MongoException> causeSupplier) {
        stateAndPermits.pause(causeSupplier);
    }

    static IllegalStateException poolClosedException() {
        return new IllegalStateException(POOL_CLOSED_MESSAGE);
    }

    static boolean isPoolClosedException(final RuntimeException e) {
        return e instanceof IllegalStateException && POOL_CLOSED_MESSAGE.equals(e.getMessage());
    }

    /**
     * Package-access methods are thread-safe,
     * and only they should be called outside of the {@link StateAndPermits}'s code.
     */
    @ThreadSafe
    private static final class StateAndPermits {
        private final ReadWriteLock lock;
        private final Condition permitAvailableOrPausedOrClosedCondition;
        private volatile boolean paused;
        private volatile boolean closed;
        private final int maxPermits;
        private volatile int permits;
        @Nullable
        Supplier<MongoException> causeSupplier;

        StateAndPermits(final int maxPermits) {
            lock = new ReentrantReadWriteLock(true);
            permitAvailableOrPausedOrClosedCondition = lock.writeLock().newCondition();
            paused = false;
            closed = false;
            this.maxPermits = maxPermits;
            permits = maxPermits;
            causeSupplier = null;
        }

        int permits() {
            return permits;
        }

        boolean acquirePermitImmediateUnfair() {
            log("acquirePermitImmediateUnfair beforeL");
            if (!lock.writeLock().tryLock()) {
                lock.writeLock().lock();
            }
            try {
                log("acquirePermitImmediateUnfair afterL");
                throwIfClosed();
                throwIfPaused();
//                if (closedOrThrowIfPaused()) {
//                    return false;
//                }
                if (permits > 0) {
                    //noinspection NonAtomicOperationOnVolatileField
                    permits--;
                    threadLocalAcquire();
                    return true;
                } else {
                    return false;
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        /**
         * @param timeout See {@link com.mongodb.internal.Timeout#startNow(long, TimeUnit)}.
         *                May return {@code false} early if {@link #closed()}.
         */
        boolean acquirePermitFair(final long timeout, final TimeUnit unit) throws MongoInterruptedException {
            long remainingNanos = unit.toNanos(timeout);
            log("acquirePermitFair beforeL");
            lock.writeLock().lock();
            try {
                log("acquirePermitFair afterL");
                while (permits == 0) {
                    throwIfClosed();
                    throwIfPaused();
//                    if (closedOrThrowIfPaused()) {
//                        return false;
//                    }
                    try {
                        if (timeout < 0 || remainingNanos == Long.MAX_VALUE) {
                            permitAvailableOrPausedOrClosedCondition.await();
                        } else if (remainingNanos >= 0) {
                            remainingNanos = permitAvailableOrPausedOrClosedCondition.awaitNanos(remainingNanos);
                        } else {
                            return false;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new MongoInterruptedException(null, e);
                    }
                    log("acquirePermitFair awakened");
                }
                assertTrue(permits > 0);
                throwIfClosed();
                throwIfPaused();
                //noinspection NonAtomicOperationOnVolatileField
                permits--;
                threadLocalAcquire();
                log("acquirePermitFair acquired");
                return true;
//                if (closedOrThrowIfPaused()) {
//                    return false;
//                } else {
//                    //noinspection NonAtomicOperationOnVolatileField
//                    permits--;
//                    return true;
//                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        void releasePermit() {
            log("releasePermit beforeL");
            lock.writeLock().lock();
            try {
                log("releasePermit afterL");
                assertTrue(permits < maxPermits);
                //noinspection NonAtomicOperationOnVolatileField
                permits++;
                permitAvailableOrPausedOrClosedCondition.signal();
                threadLocalRelease();
                log("releasePermit released");
            } finally {
                lock.writeLock().unlock();
            }
        }

        void pause(final Supplier<MongoException> causeSupplier) {
            log("pause beforeL");
            lock.writeLock().lock();
            try {
                log("pause afterL");
                if (!paused) {
                    this.paused = true;
                    permitAvailableOrPausedOrClosedCondition.signalAll();
                    log("pause signalled");
                } else {
                    log("pause");
                }
                this.causeSupplier = assertNotNull(causeSupplier);
            } finally {
                lock.writeLock().unlock();
            }
        }

        void ready() {
            if (paused) {
                log("ready beforeL");
                lock.writeLock().lock();
                try {
                    log("ready afterL");
                    this.paused = false;
                    this.causeSupplier = null;
                    log("ready");
                } finally {
                    lock.writeLock().unlock();
                }
            } else {
                log("ready skipped");
            }
        }

        /**
         * @return {@code true} if and only if the state changed as a result of the operation.
         */
        boolean close() {
            if (!closed) {
                lock.writeLock().lock();
                try {
                    if (!closed) {
                        closed = true;
                        permitAvailableOrPausedOrClosedCondition.signalAll();
                        return true;
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }
            return false;
        }

        /**
         * @throws MongoException If and only if {@linkplain #pause(Supplier) paused}. The exception is specified via the
         * {@link #pause(Supplier)} method and may be a subtype of {@link MongoException}.
         */
        void throwIfPaused() throws MongoException {
            if (paused) {
                log("throwIfPaused beforeRL");
                lock.readLock().lock();
                try {
                    log("throwIfPaused afterRL");
                    if (paused) {
                        log("throw paused");
                        throw assertNotNull(
                                assertNotNull(causeSupplier).get());
                    }
                } finally {
                    lock.readLock().unlock();
                }
            }
        }

        /**
         * @throws IllegalArgumentException If and only if {@linkplain #closed()}.
         */
        void throwIfClosed() throws IllegalStateException {
            if (paused) {
                lock.readLock().lock();
                try {
                    if (paused) {
                        throw assertNotNull(
                                assertNotNull(causeSupplier).get());
                    }
                } finally {
                    lock.readLock().unlock();
                }
            }
        }

        boolean closed() {
            return closed;
        }

        /**
         * @return {@link #closed}.
         * @throws MongoException See {@link #throwIfPaused()}.
         */
        private boolean closedOrThrowIfPaused() throws MongoException {
            throwIfPaused();
            return closed;
        }
    }

    static final long startNanos = System.nanoTime();

    public static void log(Object msg) {
//        System.out.printf("%s [%s] CP %s%n", TimeUnit.NANOSECONDS.toMillis(
//                System.nanoTime() - startNanos),
//                Thread.currentThread().getId() + " " + Thread.currentThread().getName(), msg);
    }

    static void threadLocalAcquire() {
    }

    static void threadLocalRelease() {
    }

    ConcurrentMap<ConnectionId, List<RuntimeException>> map = new ConcurrentHashMap<>();

    void gget(T element, Object msg) {
//        ConnectionId id = ((InternalConnection) element).getDescription().getConnectionId();
//        map.compute(id, (k, v) -> {
//            List<RuntimeException> result = v == null ? Arrays.asList(null, null) : new ArrayList<>(v);
//            String idAndThread = "conn=" + id + " " + Thread.currentThread().getName() + " " + Thread.currentThread().getId();
//            if (result.get(0) == null) {
//                logg(msg + "get " + id);
//                result.set(0, new RuntimeException("get " + idAndThread));
//                result.set(1, null);
//            } else {
//                logg(msg + "get second" + id);
//                RuntimeException e = new RuntimeException("get " + idAndThread);
//                e.addSuppressed(result.get(0));
//                throw e;
//            }
//            return result;
//        });
//        map.computeIfPresent(id, (k, v) -> {
//            logg(msg + " " + id);
//            return null;
//        });
    }

    void rrelease(T element) {
//        ConnectionId id = ((InternalConnection) element).getDescription().getConnectionId();
//        map.compute(id, (k, v) -> {
//            List<RuntimeException> result = v == null ? Arrays.asList(null, null) : new ArrayList<>(v);
//            String idAndThread = "conn=" + id + " " + Thread.currentThread().getName() + " " + Thread.currentThread().getId();
//            if (result.get(1) == null) {
//                logg("release " + id);
//                result.set(0, null);
//                result.set(1, new RuntimeException("release " + idAndThread));
//            } else {
//                logg("release second" + id);
//                RuntimeException e = new RuntimeException("release " + idAndThread);
//                e.addSuppressed(result.get(1));
//                throw e;
//            }
//            return result;
//        });
    }

    public static void logg(Object msg) {
//        System.out.printf("%s [%s] CP %s%n", TimeUnit.NANOSECONDS.toMillis(
//                System.nanoTime() - startNanos),
//                Thread.currentThread().getId() + " " + Thread.currentThread().getName(), msg);
    }
}
