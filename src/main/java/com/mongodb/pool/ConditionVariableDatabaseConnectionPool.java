package com.mongodb.pool;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public final class ConditionVariableDatabaseConnectionPool implements ConnectionPool {
    private final Queue<DatabaseConnection> connections;
    private final Lock lock;
    private final Condition notEmpty;
    private final boolean signalAll;

    public ConditionVariableDatabaseConnectionPool(
            int capacity, final Function<Integer, Queue<DatabaseConnection>> queueCreator, final boolean signalAll) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("The capacity must be positive.");
        }
        connections = queueCreator.apply(capacity);
        if (!connections.isEmpty()) {
            throw new IllegalArgumentException("The queue must be empty.");
        }
        for (int i = 0; i < capacity; i++) {
            connections.add(new DatabaseConnection());
        }
        lock = new ReentrantLock(false);
        notEmpty = lock.newCondition();
        this.signalAll = signalAll;
    }

    @Override
    public Connection getConnection() {
        DatabaseConnection conn;
        lock.lock();
        try {
            while (connections.isEmpty()) {
                notEmpty.awaitUninterruptibly();
            }
            conn = connections.remove();
        } finally {
            lock.unlock();
        }
        try {
            conn.open();
        } catch (Throwable e) {
            // it is crucial to add `conn` back if `open` fails
            connections.add(conn);
            throw e;
        }
        return new PooledConnection(conn);
    }

    private void returnConnection(DatabaseConnection conn) {
        lock.lock();
        try {
            connections.add(conn);
            if (signalAll) {
                notEmpty.signalAll();
            } else {
                notEmpty.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    private final class PooledConnection implements Connection {
        private DatabaseConnection wrapped;

        PooledConnection(DatabaseConnection conn) {
            wrapped = conn;
        }

        @Override
        public ByteBuffer executeCommand(final ByteBuffer cmd) {
            if (wrapped == null) {
                throw new IllegalStateException("The connection is closed.");
            }
            return wrapped.executeCommand(cmd);
        }

        @Override
        public void close() {
            if (wrapped != null) {
                ConditionVariableDatabaseConnectionPool.this.returnConnection(wrapped);
                wrapped = null;
            }
        }
    }
}
