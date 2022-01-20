package com.mongodb;

import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.connection.CommandMessage;
import com.mongodb.internal.connection.ConnectionGenerationSupplier;
import com.mongodb.internal.connection.DefaultConnectionPool;
import com.mongodb.internal.connection.InternalConnection;
import com.mongodb.internal.connection.InternalConnectionFactory;
import com.mongodb.internal.connection.ResponseBuffers;
import com.mongodb.internal.connection.SdamServerDescriptionManager;
import com.mongodb.internal.inject.SameObjectProvider;
import com.mongodb.internal.session.SessionContext;
import org.bson.ByteBuf;
import org.bson.codecs.Decoder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import static org.openjdk.jmh.runner.options.TimeValue.seconds;

public class DefaultConnectionPoolBenchmark {
    public DefaultConnectionPoolBenchmark() {
    }

    /**
     * Run:
     * <pre>{@code
     * > mvn clean verify
     * > java -cp target/benchmarks.jar com.mongodb.DefaultConnectionPoolBenchmark
     * }</pre>
     */
    public static void main(String[] args) throws RunnerException {
        ChainedOptionsBuilder opts = new OptionsBuilder()
                .shouldFailOnError(true)
                .shouldDoGC(false)
                .timeout(seconds(100))
                .jvmArgsAppend("-Xmx2G", "-server", "-disableassertions")
                .include(DefaultConnectionPoolBenchmark.class.getSimpleName() + "\\.connectionCycle")

                .mode(Mode.Throughput)
                .timeUnit(TimeUnit.MILLISECONDS)
                .forks(4)
                .warmupForks(1)
                .warmupIterations(3)
                .warmupTime(seconds(2))
                .measurementIterations(2)
                .measurementTime(seconds(5));

//                .mode(Mode.SampleTime)
//                .timeUnit(TimeUnit.MILLISECONDS)
//                .forks(6)
//                .warmupForks(1)
//                .warmupIterations(3)
//                .warmupTime(seconds(2))
//                .measurementIterations(3)
//                .measurementTime(seconds(5));

        new Runner(opts
                .threads(1)
                .build())
                .run();
        new Runner(opts
                .threads(6)
                .build())
                .run();
        new Runner(opts
                .threads(1000)
                .build())
                .run();
    }

    /**
     * 4.5.0-SNAPSHOT
     *   1000 threads 250 connections
     *   Benchmark                                        Mode  Cnt    Score    Error   Units
     *   DefaultConnectionPoolBenchmark.connectionCycle  thrpt    8  179.161 ± 60.814  ops/ms
     *
     *   1000 threads 1000 connections
     *   Benchmark                                        Mode  Cnt    Score     Error   Units
     *   DefaultConnectionPoolBenchmark.connectionCycle  thrpt    8  656.229 ± 181.564  ops/ms
     *
     *   6 threads 6 connections
     *   Benchmark                                        Mode  Cnt     Score    Error   Units
     *   DefaultConnectionPoolBenchmark.connectionCycle  thrpt    8  2066.979 ± 52.481  ops/ms
     *
     *   1 thread 1 connection
     *   Benchmark                                        Mode  Cnt     Score     Error   Units
     *   DefaultConnectionPoolBenchmark.connectionCycle  thrpt    8  3207.597 ± 183.752  ops/ms
     *
     *   1000 threads 250 connections
     *   Benchmark                                                                 Mode       Cnt    Score   Error  Units
     *   DefaultConnectionPoolBenchmark.connectionCycle                          sample  20188366    4.454 ± 0.010  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00    sample             ≈ 10⁻⁴          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50    sample             ≈ 10⁻³          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90    sample             28.574          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95    sample             31.457          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99    sample             69.075          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999   sample             77.070          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999  sample             93.061          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00    sample            126.616          ms/op
     *
     *   1000 threads 1000 connections
     *   Benchmark                                                                 Mode       Cnt    Score   Error  Units
     *   DefaultConnectionPoolBenchmark.connectionCycle                          sample  92974085    0.967 ± 0.002  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00    sample             ≈ 10⁻⁴          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50    sample             ≈ 10⁻³          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90    sample              0.001          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95    sample              0.001          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99    sample             31.556          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999   sample             45.285          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999  sample             65.864          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00    sample            120.717          ms/op
     *
     * 4.2.0_JAVA-4435
     *   1000 threads 250 connections
     *   Benchmark                                        Mode  Cnt   Score   Error   Units
     *   DefaultConnectionPoolBenchmark.connectionCycle  thrpt    8  33.639 ± 1.238  ops/ms
     *
     *   1000 threads 1000 connections
     *   Benchmark                                        Mode  Cnt     Score    Error   Units
     *   DefaultConnectionPoolBenchmark.connectionCycle  thrpt    8  2687.579 ± 91.716  ops/ms
     *
     *   6 threads 6 connections
     *   Benchmark                                        Mode  Cnt     Score    Error   Units
     *   DefaultConnectionPoolBenchmark.connectionCycle  thrpt    8  2730.714 ± 31.349  ops/ms
     *
     *   1 thread 1 connection
     *   Benchmark                                        Mode  Cnt     Score    Error   Units
     *   DefaultConnectionPoolBenchmark.connectionCycle  thrpt    8  5081.488 ± 26.004  ops/ms
     *
     *   1000 threads 250 connections
     *   Benchmark                                                                 Mode      Cnt   Score   Error  Units
     *   DefaultConnectionPoolBenchmark.connectionCycle                          sample  3051852  29.480 ± 0.002  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00    sample           27.984          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50    sample           29.295          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90    sample           29.983          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95    sample           30.671          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99    sample           34.603          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999   sample           41.091          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999  sample           42.598          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00    sample           42.795          ms/op
     *
     *   1000 threads 1000 connections
     *   Benchmark                                                                 Mode        Cnt     Score   Error  Units
     *   DefaultConnectionPoolBenchmark.connectionCycle                          sample  239469834     0.354 ± 0.003  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00    sample               ≈ 10⁻⁴          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50    sample                0.004          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90    sample                0.008          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95    sample                0.009          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99    sample                0.014          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999   sample                0.025          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999  sample              886.047          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00    sample             2071.986          ms/op
     *
     * 4.2.0-unfair_JAVA-4435
     *   1000 threads 250 connections
     *   Benchmark                                        Mode  Cnt     Score     Error   Units
     *   DefaultConnectionPoolBenchmark.connectionCycle  thrpt    8  2754.849 ± 172.705  ops/ms
     *
     *   1000 threads 1000 connections
     *   Benchmark                                        Mode  Cnt     Score     Error   Units
     *   DefaultConnectionPoolBenchmark.connectionCycle  thrpt    8  2673.850 ± 166.890  ops/ms
     *
     *   6 threads 6 connections
     *   Benchmark                                        Mode  Cnt     Score    Error   Units
     *   DefaultConnectionPoolBenchmark.connectionCycle  thrpt    8  2729.713 ± 60.227  ops/ms
     *
     *   1 thread 1 connection
     *   Benchmark                                        Mode  Cnt     Score     Error   Units
     *   DefaultConnectionPoolBenchmark.connectionCycle  thrpt    8  5130.801 ± 116.675  ops/ms
     *
     *   1000 threads 250 connections
     *   Benchmark                                                                 Mode        Cnt     Score   Error  Units
     *   DefaultConnectionPoolBenchmark.connectionCycle                          sample  269728278     0.383 ± 0.003  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00    sample               ≈ 10⁻⁵          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50    sample                0.004          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90    sample                0.007          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95    sample                0.009          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99    sample                0.014          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999   sample               73.138          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999  sample              590.348          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00    sample             2122.318          ms/op
     *
     *   1000 threads 1000 connections
     *   Benchmark                                                                 Mode        Cnt     Score   Error  Units
     *   DefaultConnectionPoolBenchmark.connectionCycle                          sample  238800852     0.358 ± 0.004  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00    sample               ≈ 10⁻⁴          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50    sample                0.004          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90    sample                0.008          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95    sample                0.009          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99    sample                0.014          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999   sample                0.027          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999  sample              917.504          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00    sample             2048.918          ms/op
     */
    @Benchmark
    public InternalConnection connectionCycle(final BenchmarkPoolState state) {
        InternalConnection conn = state.pool.get(4, TimeUnit.SECONDS);
        try {
            return conn;
        } finally {
            conn.close();
        }
    }

    /**
     * 1000 threads 250 permits
     * Benchmark                                                 (fair)    Mode        Cnt     Score   Error  Units
     * DefaultConnectionPoolBenchmark.semCycle                    false  sample  518906043     0.158 ± 0.001  ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.00     false  sample               ≈ 10⁻⁶          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.50     false  sample                0.002          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.90     false  sample                0.003          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.95     false  sample                0.004          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.99     false  sample                0.006          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.999    false  sample                0.009          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.9999   false  sample              581.960          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p1.00     false  sample             1591.738          ms/op
     * DefaultConnectionPoolBenchmark.semCycle                     true  sample    3118692    28.872 ± 0.002  ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.00      true  sample               27.656          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.50      true  sample               28.672          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.90      true  sample               29.458          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.95      true  sample               30.114          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.99      true  sample               33.456          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.999     true  sample               38.470          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.9999    true  sample               45.154          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p1.00      true  sample               45.482          ms/op
     *
     * 1000 threads 1000 permits
     * Benchmark                                                 (fair)    Mode        Cnt     Score   Error  Units
     * DefaultConnectionPoolBenchmark.semCycle                    false  sample  520663281     0.153 ± 0.002  ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.00     false  sample               ≈ 10⁻⁶          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.50     false  sample                0.002          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.90     false  sample                0.003          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.95     false  sample                0.004          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.99     false  sample                0.005          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.999    false  sample                0.008          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.9999   false  sample              842.007          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p1.00     false  sample             1201.668          ms/op
     * DefaultConnectionPoolBenchmark.semCycle                     true  sample  518067111     0.156 ± 0.002  ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.00      true  sample               ≈ 10⁻⁶          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.50      true  sample                0.002          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.90      true  sample                0.003          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.95      true  sample                0.004          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.99      true  sample                0.006          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.999     true  sample                0.009          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.9999    true  sample              842.007          ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p1.00      true  sample             1247.805          ms/op
     *
     * 6 threads 6 permits
     * Benchmark                                                 (fair)    Mode       Cnt   Score    Error  Units
     * DefaultConnectionPoolBenchmark.semCycle                    false  sample  17594306   0.001 ±  0.001  ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.00     false  sample            ≈ 10⁻⁶           ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.50     false  sample             0.001           ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.90     false  sample             0.002           ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.95     false  sample             0.002           ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.99     false  sample             0.003           ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.999    false  sample             0.007           ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.9999   false  sample             0.023           ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p1.00     false  sample             0.352           ms/op
     * DefaultConnectionPoolBenchmark.semCycle                     true  sample  18858359   0.001 ±  0.001  ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.00      true  sample            ≈ 10⁻⁶           ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.50      true  sample             0.001           ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.90      true  sample             0.002           ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.95      true  sample             0.002           ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.99      true  sample             0.003           ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.999     true  sample             0.007           ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p0.9999    true  sample             0.023           ms/op
     * DefaultConnectionPoolBenchmark.semCycle:semCycle·p1.00      true  sample            10.224           ms/op
     */
    @Benchmark
    public boolean semCycle(final BenchmarkState state) throws InterruptedException {
        boolean acquired = state.sem.tryAcquire(4, TimeUnit.SECONDS);
        if (!acquired) {
            throw new AssertionError();
        }
        try {
            return state.fair;
        } finally {
            state.sem.release();
        }
    }

    @Benchmark
    public boolean lockSemCycle(final BenchmarkState state) {
        boolean acquired = state.lockSem.acquire(4, TimeUnit.SECONDS);
        if (!acquired) {
            throw new AssertionError();
        }
        try {
            return state.fair;
        } finally {
            state.lockSem.release();
        }
    }

    @Benchmark
    public boolean lockCycle(final BenchmarkState state) throws InterruptedException {
        boolean acquired = state.lock.tryLock(4, TimeUnit.SECONDS);
        if (!acquired) {
            throw new AssertionError();
        }
        try {
            return state.fair;
        } finally {
            state.lock.unlock();
        }
    }

    @Benchmark
    public boolean rwlCycle(final BenchmarkState state) throws InterruptedException {
        boolean acquired = state.rwl.writeLock().tryLock(4, TimeUnit.SECONDS);
        if (!acquired) {
            throw new AssertionError();
        }
        try {
            return state.fair;
        } finally {
            state.rwl.writeLock().unlock();
        }
    }

    @State(Scope.Benchmark)
    public static class BenchmarkPoolState {
        DefaultConnectionPool pool;

        public BenchmarkPoolState() {
        }

        @Setup(Level.Trial)
        public final void setupTrial(BenchmarkParams params) {
            int maxSize = switch (params.getMode()) {
                case Throughput -> params.getThreads();
                case SampleTime -> params.getThreads() / 4;
                default -> throw new AssertionError();
            };
            {
                // 4.5.0
                pool = new DefaultConnectionPool(
                        new ServerId(new ClusterId(), new ServerAddress()),
                        new SimpleInternalConnectionFactory_4_5(maxSize),
                        ConnectionPoolSettings.builder()
                                .maxSize(maxSize)
                                .maintenanceInitialDelay(Long.MAX_VALUE, TimeUnit.NANOSECONDS)
                                .build(),
                        SameObjectProvider.initialized(new SimpleSdamServerDescriptionManager()));
                pool.ready();
            }
//            {
//                // 4.3.0, 4.2.0
//                pool = new DefaultConnectionPool(
//                        new ServerId(new ClusterId(), new ServerAddress()),
//                        new SimpleInternalConnectionFactory_4_2(maxSize),
//                        ConnectionPoolSettings.builder()
//                                .maxSize(maxSize)
//                                .maintenanceInitialDelay(Long.MAX_VALUE, TimeUnit.NANOSECONDS)
//                                .build());
//            }
            List<InternalConnection> conns = new ArrayList<>(maxSize);
            for (int i = 0; i < maxSize; i++) {
                conns.add(pool.get());
            }
            conns.forEach(InternalConnection::close);
        }

        @TearDown(Level.Trial)
        public final void tearDownTrial() {
            pool.close();
        }
    }

    /**
     * 1000 threads
     * Benchmark                                    (fair)   Mode  Cnt      Score      Error   Units
     * DefaultConnectionPoolBenchmark.lockCycle      false  thrpt    6  34681.345 ± 3310.639  ops/ms
     * DefaultConnectionPoolBenchmark.lockCycle       true  thrpt    6     36.146 ±    1.122  ops/ms
     * DefaultConnectionPoolBenchmark.rwlCycle       false  thrpt    6  31385.756 ± 2245.434  ops/ms
     * DefaultConnectionPoolBenchmark.rwlCycle        true  thrpt    6     36.076 ±    0.728  ops/ms
     *
     * DefaultConnectionPoolBenchmark.lockSemCycle   false  thrpt    6  11341.258 ± 4033.863  ops/ms
     * DefaultConnectionPoolBenchmark.lockSemCycle    true  thrpt    6     18.412 ±    0.366  ops/ms
     * DefaultConnectionPoolBenchmark.rwlSemCycle    false  thrpt    6  11834.143 ±  861.853  ops/ms
     * DefaultConnectionPoolBenchmark.rwlSemCycle     true  thrpt    6     18.468 ±    0.352  ops/ms
     * DefaultConnectionPoolBenchmark.semCycle       false  thrpt    6   5461.167 ±  826.918  ops/ms
     * DefaultConnectionPoolBenchmark.semCycle        true  thrpt    6   5591.832 ±  595.375  ops/ms
     *
     * 6 threads
     * Benchmark                                    (fair)   Mode  Cnt      Score      Error   Units
     * DefaultConnectionPoolBenchmark.lockCycle      false  thrpt    6  38290.078 ± 3834.917  ops/ms
     * DefaultConnectionPoolBenchmark.lockCycle       true  thrpt    6    238.625 ±    2.841  ops/ms
     * DefaultConnectionPoolBenchmark.rwlCycle       false  thrpt    6  42521.313 ± 2023.284  ops/ms
     * DefaultConnectionPoolBenchmark.rwlCycle        true  thrpt    6    238.764 ±    4.098  ops/ms
     *
     * DefaultConnectionPoolBenchmark.lockSemCycle   false  thrpt    6  16076.607 ± 1316.434  ops/ms
     * DefaultConnectionPoolBenchmark.lockSemCycle    true  thrpt    6    128.140 ±    1.987  ops/ms
     * DefaultConnectionPoolBenchmark.rwlSemCycle    false  thrpt    6  15361.978 ± 1948.505  ops/ms
     * DefaultConnectionPoolBenchmark.rwlSemCycle     true  thrpt    6    128.967 ±    1.257  ops/ms
     * DefaultConnectionPoolBenchmark.semCycle       false  thrpt    6   7325.350 ± 2092.392  ops/ms
     * DefaultConnectionPoolBenchmark.semCycle        true  thrpt    6   7021.181 ±  165.662  ops/ms
     *
     * 1 thread
     * Benchmark                                    (fair)   Mode  Cnt      Score      Error   Units
     * DefaultConnectionPoolBenchmark.lockCycle      false  thrpt    6  64682.538 ±  807.389  ops/ms
     * DefaultConnectionPoolBenchmark.lockCycle       true  thrpt    6  58062.946 ±  486.892  ops/ms
     * DefaultConnectionPoolBenchmark.rwlCycle       false  thrpt    6  62465.997 ± 1486.069  ops/ms
     * DefaultConnectionPoolBenchmark.rwlCycle        true  thrpt    6  63179.493 ±  659.891  ops/ms
     *
     * DefaultConnectionPoolBenchmark.lockSemCycle   false  thrpt    6  27841.200 ±  728.562  ops/ms
     * DefaultConnectionPoolBenchmark.lockSemCycle    true  thrpt    6  23613.008 ±  618.165  ops/ms
     * DefaultConnectionPoolBenchmark.rwlSemCycle    false  thrpt    6  23026.466 ±  293.248  ops/ms
     * DefaultConnectionPoolBenchmark.rwlSemCycle     true  thrpt    6  23156.111 ±  533.913  ops/ms
     * DefaultConnectionPoolBenchmark.semCycle       false  thrpt    6  64088.733 ±  706.289  ops/ms
     * DefaultConnectionPoolBenchmark.semCycle        true  thrpt    6  64075.289 ±  733.837  ops/ms
     */
    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Param({"false", "true"})
        public boolean fair;
        ReentrantLock lock;
        ReentrantReadWriteLock rwl;
        Semaphore sem;
        LockSem lockSem;

        public BenchmarkState() {
        }

        @Setup(Level.Trial)
        public final void setupTrial(BenchmarkParams params) {
            lock = new ReentrantLock(fair);
            rwl = new ReentrantReadWriteLock(fair);
            int permits = switch (params.getMode()) {
                case Throughput -> params.getThreads();
                case SampleTime -> params.getThreads() / 4;
                default -> throw new AssertionError();
            };
            sem = new Semaphore(permits, fair);
            lockSem = new LockSem(permits, new ReentrantLock(fair));
        }
    }

    @ThreadSafe
    private static final class SimpleInternalConnection implements InternalConnection {
        private final AtomicBoolean open = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();
        private final ConnectionDescription desc = new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress()));
        private final ServerDescription sd = ServerDescription.builder()
                .address(new ServerAddress())
                .state(ServerConnectionState.CONNECTED)
                .build();
        private final int generation;

        SimpleInternalConnection(final int generation) {
            this.generation = generation;
        }

        @Override
        public ConnectionDescription getDescription() {
            return desc;
        }

        @Override
        public ServerDescription getInitialServerDescription() {
            return sd;
        }

        @Override
        public void open() {
            open.set(true);
        }

        @Override
        public void openAsync(final SingleResultCallback<Void> callback) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            closed.set(true);
        }

        @Override
        public boolean opened() {
            return open.get();
        }

        @Override
        public boolean isClosed() {
            return closed.get();
        }

        @Override
        public int getGeneration() {
            return generation;
        }

        @Override
        public <T> T sendAndReceive(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext
                , final RequestContext requestContext
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> void send(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T receive(final Decoder<T> decoder, final SessionContext sessionContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasMoreToCome() {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> void sendAndReceiveAsync(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext
                , final RequestContext requestContext
                , final SingleResultCallback<T> callback) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ResponseBuffers receiveMessage(final int responseTo) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuf getBuffer(final int size) {
            throw new UnsupportedOperationException();
        }
    }

    @ThreadSafe
    private static final class SimpleSdamServerDescriptionManager implements SdamServerDescriptionManager {
        SimpleSdamServerDescriptionManager() {
        }

        @Override
        public void update(final ServerDescription candidateDescription) {
        }

        @Override
        public void handleExceptionBeforeHandshake(final SdamIssue sdamIssue) {
        }

        @Override
        public void handleExceptionAfterHandshake(final SdamIssue sdamIssue) {
        }

        @Override
        public SdamIssue.Context context() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SdamServerDescriptionManager.SdamIssue.Context context(final InternalConnection connection) {
            throw new UnsupportedOperationException();
        }
    }

    @ThreadSafe
    private static final class SimpleInternalConnectionFactory_4_5 implements InternalConnectionFactory {
        private final int maxSize;
        private final AtomicInteger count = new AtomicInteger();

        SimpleInternalConnectionFactory_4_5(final int maxSize) {
            this.maxSize = maxSize;
        }

        @Override
        public InternalConnection create(final ServerId serverId, ConnectionGenerationSupplier connectionGenerationSupplier) {
            if (count.getAndIncrement() > maxSize) {
                throw new AssertionError();
            }
            return new SimpleInternalConnection(connectionGenerationSupplier.getGeneration());
        }
    }

//    @ThreadSafe
//    private static final class SimpleInternalConnectionFactory_4_2 implements InternalConnectionFactory {
//        private final int maxSize;
//        private final AtomicInteger count = new AtomicInteger();
//
//        SimpleInternalConnectionFactory_4_2(final int maxSize) {
//            this.maxSize = maxSize;
//        }
//
//        @Override
//        public InternalConnection create(final ServerId serverId) {
//            if (count.getAndIncrement() > maxSize) {
//                throw new AssertionError();
//            }
//            return new SimpleInternalConnection(0);
//        }
//    }

    @ThreadSafe
    private static final class LockSem {
        private final Lock lock;
        private final Condition permitAvailableCondition;
        private final int maxPermits;
        private volatile int permits;

        LockSem(final int maxPermits, final Lock lock) {
            this.lock = lock;
            permitAvailableCondition = lock.newCondition();
            this.maxPermits = maxPermits;
            permits = maxPermits;
        }

        boolean acquire(final long timeout, final TimeUnit unit) throws MongoInterruptedException {
            long remainingNanos = unit.toNanos(timeout);
            try {
                lock.lockInterruptibly();
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("", e);
            }
            try {
                while (permits == 0) {
                    try {
                        if (timeout < 0 || remainingNanos == Long.MAX_VALUE) {
                            permitAvailableCondition.await();
                        } else if (remainingNanos >= 0) {
                            remainingNanos = permitAvailableCondition.awaitNanos(remainingNanos);
                        } else {
                            return false;
                        }
                    } catch (InterruptedException e) {
                        throw new MongoInterruptedException("", e);
                    }
                }
                if (permits <= 0) {
                    throw new AssertionError();
                }
                //noinspection NonAtomicOperationOnVolatileField
                permits--;
                return true;
            } finally {
                lock.unlock();
            }
        }

        void release() {
            lock.lock();
            try {
                if (permits >= maxPermits) {
                    throw new AssertionError();
                }
                //noinspection NonAtomicOperationOnVolatileField
                permits++;
                permitAvailableCondition.signal();
            } finally {
                lock.unlock();
            }
        }
    }
}
