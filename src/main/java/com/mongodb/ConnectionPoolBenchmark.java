package com.mongodb;

import com.mongodb.pool.BlockingQueueDatabaseConnectionPool;
import com.mongodb.pool.ConditionVariableDatabaseConnectionPool;
import com.mongodb.pool.ObjectConditionVariableDatabaseConnectionPool;
import com.mongodb.pool.Connection;
import com.mongodb.pool.ConnectionPool;
import com.mongodb.pool.SemaphoreDatabaseConnectionPool;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static org.openjdk.jmh.runner.options.TimeValue.seconds;

/**
 * 6 threads
 * Benchmark                                (contended)                                 (poolKind)   Mode  Cnt      Score     Error   Units
 * ConnectionPoolBenchmark.connectionCycle        false          SEMAPHORE_CONCURRENT_LINKED_QUEUE  thrpt   12   5171.966 ± 198.925  ops/ms
 * ConnectionPoolBenchmark.connectionCycle        false             SEMAPHORE_ARRAY_BLOCKING_QUEUE  thrpt   12  11998.457 ± 285.917  ops/ms
 * ConnectionPoolBenchmark.connectionCycle        false                      LINKED_BLOCKING_QUEUE  thrpt   12   8087.702 ± 370.333  ops/ms
 * ConnectionPoolBenchmark.connectionCycle        false                       ARRAY_BLOCKING_QUEUE  thrpt   12  16369.499 ± 590.140  ops/ms
 * ConnectionPoolBenchmark.connectionCycle        false             CONDITION_VARIABLE_ARRAY_DEQUE  thrpt   12  16419.492 ± 387.159  ops/ms
 * ConnectionPoolBenchmark.connectionCycle        false  SIGNAL_ALL_CONDITION_VARIABLE_ARRAY_DEQUE  thrpt   12  16759.132 ± 685.228  ops/ms
 * ConnectionPoolBenchmark.connectionCycle        false      OBJECT_CONDITION_VARIABLE_ARRAY_DEQUE  thrpt   12   6948.064 ± 420.384  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true          SEMAPHORE_CONCURRENT_LINKED_QUEUE  thrpt   12   4472.347 ± 130.202  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true             SEMAPHORE_ARRAY_BLOCKING_QUEUE  thrpt   12   5795.609 ± 137.059  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true                      LINKED_BLOCKING_QUEUE  thrpt   12   8153.985 ± 420.685  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true                       ARRAY_BLOCKING_QUEUE  thrpt   12  14147.689 ± 507.571  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true             CONDITION_VARIABLE_ARRAY_DEQUE  thrpt   12  14473.062 ± 527.876  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true  SIGNAL_ALL_CONDITION_VARIABLE_ARRAY_DEQUE  thrpt   12  14531.928 ± 720.741  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true      OBJECT_CONDITION_VARIABLE_ARRAY_DEQUE  thrpt   12   7289.176 ± 424.693  ops/ms
 *
 * 60 threads
 * Benchmark                                (contended)                                 (poolKind)   Mode  Cnt      Score      Error   Units
 * ConnectionPoolBenchmark.connectionCycle        false          SEMAPHORE_CONCURRENT_LINKED_QUEUE  thrpt   12   4959.568 ±  242.154  ops/ms
 * ConnectionPoolBenchmark.connectionCycle        false             SEMAPHORE_ARRAY_BLOCKING_QUEUE  thrpt   12  11537.331 ±  597.124  ops/ms
 * ConnectionPoolBenchmark.connectionCycle        false                      LINKED_BLOCKING_QUEUE  thrpt   12   6654.689 ±  317.752  ops/ms
 * ConnectionPoolBenchmark.connectionCycle        false                       ARRAY_BLOCKING_QUEUE  thrpt   12  15682.089 ± 1244.628  ops/ms
 * ConnectionPoolBenchmark.connectionCycle        false             CONDITION_VARIABLE_ARRAY_DEQUE  thrpt   12  16389.859 ±  523.985  ops/ms
 * ConnectionPoolBenchmark.connectionCycle        false  SIGNAL_ALL_CONDITION_VARIABLE_ARRAY_DEQUE  thrpt   12  16682.471 ±  714.567  ops/ms
 * ConnectionPoolBenchmark.connectionCycle        false      OBJECT_CONDITION_VARIABLE_ARRAY_DEQUE  thrpt   12   6462.509 ±  553.754  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true          SEMAPHORE_CONCURRENT_LINKED_QUEUE  thrpt   12   4992.908 ±  161.260  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true             SEMAPHORE_ARRAY_BLOCKING_QUEUE  thrpt   12   5565.355 ±  286.818  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true                      LINKED_BLOCKING_QUEUE  thrpt   12   6361.535 ±  216.964  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true                       ARRAY_BLOCKING_QUEUE  thrpt   12  15609.711 ±  705.534  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true             CONDITION_VARIABLE_ARRAY_DEQUE  thrpt   12  16490.352 ±  713.968  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true  SIGNAL_ALL_CONDITION_VARIABLE_ARRAY_DEQUE  thrpt   12  15804.028 ±  932.432  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true      OBJECT_CONDITION_VARIABLE_ARRAY_DEQUE  thrpt   12   6918.850 ±  481.146  ops/ms
 *
 * 600 threads
 * Benchmark                                (contended)                                 (poolKind)   Mode  Cnt      Score      Error   Units
 * ConnectionPoolBenchmark.connectionCycle        false          SEMAPHORE_CONCURRENT_LINKED_QUEUE  thrpt   12   4791.257 ±  211.736  ops/ms
 * ConnectionPoolBenchmark.connectionCycle        false             SEMAPHORE_ARRAY_BLOCKING_QUEUE  thrpt   12   7609.910 ± 1089.065  ops/ms
 * ConnectionPoolBenchmark.connectionCycle        false                      LINKED_BLOCKING_QUEUE  thrpt   12   3372.620 ±  947.707  ops/ms
 * ConnectionPoolBenchmark.connectionCycle        false                       ARRAY_BLOCKING_QUEUE  thrpt   12  13035.286 ± 1322.205  ops/ms
 * ConnectionPoolBenchmark.connectionCycle        false             CONDITION_VARIABLE_ARRAY_DEQUE  thrpt   12  12455.252 ± 1702.206  ops/ms
 * ConnectionPoolBenchmark.connectionCycle        false  SIGNAL_ALL_CONDITION_VARIABLE_ARRAY_DEQUE  thrpt   12  12889.458 ± 1108.149  ops/ms
 * ConnectionPoolBenchmark.connectionCycle        false      OBJECT_CONDITION_VARIABLE_ARRAY_DEQUE  thrpt   12   7792.405 ± 1865.081  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true          SEMAPHORE_CONCURRENT_LINKED_QUEUE  thrpt   12   4861.635 ±  218.155  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true             SEMAPHORE_ARRAY_BLOCKING_QUEUE  thrpt   12   3846.611 ±  358.404  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true                      LINKED_BLOCKING_QUEUE  thrpt   12   2848.436 ±  639.925  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true                       ARRAY_BLOCKING_QUEUE  thrpt   12  12494.818 ± 1445.065  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true             CONDITION_VARIABLE_ARRAY_DEQUE  thrpt   12  12395.754 ± 1330.421  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true  SIGNAL_ALL_CONDITION_VARIABLE_ARRAY_DEQUE  thrpt   12  12449.577 ± 1004.558  ops/ms
 * ConnectionPoolBenchmark.connectionCycle         true      OBJECT_CONDITION_VARIABLE_ARRAY_DEQUE  thrpt   12   5856.767 ±  575.679  ops/ms
 */
public class ConnectionPoolBenchmark {
    public ConnectionPoolBenchmark() {
    }

    /**
     * Run:
     * <pre>{@code
     * > mvn clean verify
     * > java -cp target/benchmarks.jar com.mongodb.ConnectionPoolBenchmark
     * }</pre>
     */
    public static void main(String[] args) {
        Supplier<ChainedOptionsBuilder> commonOpts = () -> new OptionsBuilder()
                .shouldFailOnError(true)
                .timeout(seconds(100))
                .jvmArgsAppend(
                        "-Xms1G",
                        "-Xmx1G",
                        "-server"
                )
                .include(ConnectionPoolBenchmark.class.getSimpleName() + "\\.connectionCycle")
                .forks(4)
                .warmupForks(0)
                .warmupIterations(4)
                .warmupTime(seconds(2))
                .measurementIterations(3)
                .measurementTime(seconds(5));
        Collection<Integer> threadCounts = asList(
                600,
                60,
                6
        );
        Consumer<ChainedOptionsBuilder> runner = options -> {
            for (int threadCount : threadCounts) {
                try {
                    new Runner(options.threads(threadCount).build()).run();
                } catch (RunnerException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        runner.accept(commonOpts.get()
                .mode(Mode.Throughput)
                .timeUnit(TimeUnit.MILLISECONDS));
    }

    @Benchmark
    public Connection connectionCycle(final BenchmarkState state) {
        try (Connection conn = state.pool.getConnection()) {
            return conn;
        }
    }

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Param({
                "false",
                "true"
        })
        public boolean contended;
        @Param({
                "SEMAPHORE_CONCURRENT_LINKED_QUEUE",
                "SEMAPHORE_ARRAY_BLOCKING_QUEUE",
                "LINKED_BLOCKING_QUEUE",
                "ARRAY_BLOCKING_QUEUE",
                "CONDITION_VARIABLE_ARRAY_DEQUE",
                "SIGNAL_ALL_CONDITION_VARIABLE_ARRAY_DEQUE",
                "OBJECT_CONDITION_VARIABLE_ARRAY_DEQUE"
        })
        public PoolKind poolKind;
        ConnectionPool pool;

        public BenchmarkState() {
        }

        @Setup(Level.Trial)
        public final void setupTrial(BenchmarkParams params) {
            int threads = params.getThreads();
            int capacity = contended
                    ? threads / 2
                    : threads + 1;
            pool = poolKind.poolCreator.apply(capacity);
        }

        public enum PoolKind {
            SEMAPHORE_CONCURRENT_LINKED_QUEUE(capacity ->
                    new SemaphoreDatabaseConnectionPool(capacity, c -> new ConcurrentLinkedQueue<>())),
            SEMAPHORE_ARRAY_BLOCKING_QUEUE(capacity ->
                    new SemaphoreDatabaseConnectionPool(capacity, c -> new ArrayBlockingQueue<>(c, false))),
            LINKED_BLOCKING_QUEUE(capacity ->
                    new BlockingQueueDatabaseConnectionPool(capacity, LinkedBlockingQueue::new)),
            ARRAY_BLOCKING_QUEUE(capacity ->
                    new BlockingQueueDatabaseConnectionPool(capacity,c -> new ArrayBlockingQueue<>(c, false))),
            CONDITION_VARIABLE_ARRAY_DEQUE(capacity ->
                    new ConditionVariableDatabaseConnectionPool(capacity, ArrayDeque::new, false)),
            SIGNAL_ALL_CONDITION_VARIABLE_ARRAY_DEQUE(capacity ->
                    new ConditionVariableDatabaseConnectionPool(capacity, ArrayDeque::new, true)),
            OBJECT_CONDITION_VARIABLE_ARRAY_DEQUE(capacity ->
                    new ObjectConditionVariableDatabaseConnectionPool(capacity, ArrayDeque::new));

            private final Function<Integer, ConnectionPool> poolCreator;

            PoolKind(final Function<Integer, ConnectionPool> poolCreator) {
                this.poolCreator = poolCreator;
            }
        }
    }
}
