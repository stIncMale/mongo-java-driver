package com.mongodb.pool;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

public final class SemaphoreDatabaseConnectionPool implements ConnectionPool {
    private final Queue<DatabaseConnection> connections;
    private final Semaphore permits;

    public SemaphoreDatabaseConnectionPool(int capacity, final Function<Integer, Queue<DatabaseConnection>> threadSafeQueueCreator) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("The capacity must be positive.");
        }
        connections = threadSafeQueueCreator.apply(capacity);
        if (!connections.isEmpty()) {
            throw new IllegalArgumentException("The queue must be empty.");
        }
        permits = new Semaphore(capacity, false);
    }

    @Override
    public Connection getConnection() {
        permits.acquireUninterruptibly();
        DatabaseConnection conn = connections.poll();
        if (conn == null) {
            //noinspection resource
            DatabaseConnection newConn = new DatabaseConnection();
            try {
                newConn.open();
            } catch (Throwable e) {
                // it is crucial to release a permit back if `open` fails
                permits.release();
                throw e;
            }
            conn = newConn;
        }
        return new PooledConnection(conn);
    }

    private void returnConnection(DatabaseConnection conn) {
        connections.add(conn);
        // it is crucial to release a permit after (not before) adding `conn` to `connections`
        permits.release();
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
                SemaphoreDatabaseConnectionPool.this.returnConnection(wrapped);
                wrapped = null;
            }
        }
    }
}
