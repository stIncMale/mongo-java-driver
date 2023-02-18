package com.mongodb.pool;

import com.mongodb.annotations.ThreadSafe;

/**
 * Assume {@link ConnectionPool}
 * is defined elsewhere and cannot be changed.
 */
@ThreadSafe
public interface ConnectionPool {
    Connection getConnection();
}
