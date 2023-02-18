package com.mongodb.pool;

import java.nio.ByteBuffer;

/**
 * Assume this implementation of {@link Connection}
 * is defined elsewhere and cannot be changed.
 */
public final class DatabaseConnection implements Connection {
    private State state;

    /**
     * Does not do I/O.
     * {@link #open()} must be called to enable
     * the use of {@link #executeCommand(ByteBuffer)}.
     */
    DatabaseConnection() {
        state = State.UNINITIALIZED;
    }

    /**
     * Does all the work needed, including I/O,
     * to enable the use of {@link #executeCommand(ByteBuffer)}.
     * The method may be called multiple times
     * regardless of whether it fails or not.
     * Once the method returns successfully,
     * subsequent calls do nothing.
     */
    void open() {
        switch (state) {
            case OPEN -> {
                // do nothing
            }
            case UNINITIALIZED -> {
                doOpen();
                state = State.OPEN;
            }
            case CLOSED -> throw new IllegalStateException("The connection is closed.");
        }
    }

    @Override
    public void close() {
        switch (state) {
            case CLOSED -> {
                // do nothing
            }
            case UNINITIALIZED, OPEN -> {
                state = State.CLOSED;
                doClose();
            }
        }
    }

    @Override
    public ByteBuffer executeCommand(ByteBuffer cmd) {
        if (state != State.OPEN) {
            throw new IllegalStateException("The connection is not open.");
        }
        return ByteBuffer.wrap(new byte[0]);
    }

    private void doOpen() {
    }

    private void doClose() {
    }

    private enum State {
        UNINITIALIZED,
        OPEN,
        CLOSED
    }
}
