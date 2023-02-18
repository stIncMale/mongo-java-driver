package com.mongodb.pool;

import java.nio.ByteBuffer;

/**
 * Assume {@link Connection}
 * is defined elsewhere and cannot be changed.
 */
public interface Connection extends AutoCloseable {
    ByteBuffer executeCommand(ByteBuffer cmd);

    /**
     * This method is idempotent.
     */
    void close();
}
