package com.mongodb.pool;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;

public final class BlockingQueueDatabaseConnectionPool implements ConnectionPool {
    private final BlockingQueue<DatabaseConnection> connections;

    public BlockingQueueDatabaseConnectionPool(int capacity, final Function<Integer, BlockingQueue<DatabaseConnection>> queueCreator) {
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
    }

    @Override
    public Connection getConnection() {
        DatabaseConnection conn;
        try {
            conn = connections.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
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
        connections.add(conn);
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
                BlockingQueueDatabaseConnectionPool.this.returnConnection(wrapped);
                wrapped = null;
            }
        }
    }
}
