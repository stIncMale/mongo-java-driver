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
import java.util.function.Supplier;

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
        Supplier<ChainedOptionsBuilder> commonOpts = () -> new OptionsBuilder()
                .shouldFailOnError(true)
                .shouldDoGC(false)
                .timeout(seconds(100))
                .jvmArgsAppend("-Xmx2G", "-server", "-disableassertions")
                .include(DefaultConnectionPoolBenchmark.class.getSimpleName() + "\\.connectionCycle");
        {
            ChainedOptionsBuilder opts = commonOpts.get()
                    .mode(Mode.Throughput)
                    .timeUnit(TimeUnit.MILLISECONDS)
                    .forks(4)
                    .warmupForks(1)
                    .warmupIterations(3)
                    .warmupTime(seconds(2))
                    .measurementIterations(2)
                    .measurementTime(seconds(3));
            new Runner(opts
                    .threads(1000)
                    .build())
                    .run();
            new Runner(opts
                    .threads(6)
                    .build())
                    .run();
        }
        {
            ChainedOptionsBuilder opts = commonOpts.get()
                    .mode(Mode.SampleTime)
                    .timeUnit(TimeUnit.MILLISECONDS)
                    .forks(4)
                    .warmupForks(1)
                    .warmupIterations(3)
                    .warmupTime(seconds(2))
                    .measurementIterations(3)
                    .measurementTime(seconds(5));
            new Runner(opts
                    .threads(1000)
                    .build())
                    .run();
            new Runner(opts
                    .threads(6)
                    .build())
                    .run();
        }
    }

    /**
     * 4.5.0-snapshot-proposed
     * 1000 threads
     *   Benchmark                                       (contended)   Mode  Cnt    Score     Error   Units
     *   DefaultConnectionPoolBenchmark.connectionCycle        false  thrpt    8  797.677 ± 165.575  ops/ms
     *   DefaultConnectionPoolBenchmark.connectionCycle         true  thrpt    8  181.470 ±  77.841  ops/ms
     *
     *   Benchmark                                                               (contended)    Mode       Cnt    Score   Error  Units
     *   DefaultConnectionPoolBenchmark.connectionCycle                                false  sample  57664407    1.040 ± 0.003  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00          false  sample             ≈ 10⁻⁴          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50          false  sample             ≈ 10⁻³          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90          false  sample              0.001          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95          false  sample              0.001          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99          false  sample             30.802          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999         false  sample             59.310          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999        false  sample             66.519          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00          false  sample             96.600          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle                                 true  sample  15600263    3.842 ± 0.010  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00           true  sample             ≈ 10⁻⁴          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50           true  sample             ≈ 10⁻³          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90           true  sample             21.430          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95           true  sample             30.245          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99           true  sample             65.536          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999          true  sample             74.449          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999         true  sample             89.784          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00           true  sample            133.169          ms/op
     *
     * 6 threads
     *   Benchmark                                       (contended)   Mode  Cnt     Score     Error   Units
     *   DefaultConnectionPoolBenchmark.connectionCycle        false  thrpt    8  2055.452 ± 119.614  ops/ms
     *   DefaultConnectionPoolBenchmark.connectionCycle         true  thrpt    8   661.753 ±  55.041  ops/ms
     *
     *   Benchmark                                                               (contended)    Mode      Cnt   Score    Error  Units
     *   DefaultConnectionPoolBenchmark.connectionCycle                                false  sample  7365311   0.003 ±  0.001  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00          false  sample           ≈ 10⁻⁴           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50          false  sample           ≈ 10⁻³           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90          false  sample            0.018           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95          false  sample            0.020           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99          false  sample            0.023           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999         false  sample            0.057           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999        false  sample            0.117           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00          false  sample           12.190           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle                                 true  sample  8912406   0.010 ±  0.001  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00           true  sample           ≈ 10⁻⁴           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50           true  sample           ≈ 10⁻³           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90           true  sample            0.025           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95           true  sample            0.048           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99           true  sample            0.059           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999          true  sample            0.109           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999         true  sample            0.166           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00           true  sample            4.710           ms/op
     *
     * 4.5.0-snapshot-permits
     * 1000 threads
     *   Benchmark                                       (contended)   Mode  Cnt    Score     Error   Units
     *   DefaultConnectionPoolBenchmark.connectionCycle        false  thrpt    8  688.735 ± 137.663  ops/ms
     *   DefaultConnectionPoolBenchmark.connectionCycle         true  thrpt    8  474.702 ±  35.804  ops/ms
     *
     *   Benchmark                                                               (contended)    Mode       Cnt     Score   Error  Units
     *   DefaultConnectionPoolBenchmark.connectionCycle                                false  sample  57501486     1.044 ± 0.003  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00          false  sample              ≈ 10⁻⁴          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50          false  sample              ≈ 10⁻³          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90          false  sample               0.001          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95          false  sample               0.001          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99          false  sample              30.835          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999         false  sample              50.201          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999        false  sample              64.815          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00          false  sample             105.251          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle                                 true  sample  26632249     2.276 ± 0.011  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00           true  sample              ≈ 10⁻⁴          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50           true  sample              ≈ 10⁻³          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90           true  sample               0.001          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95           true  sample               0.001          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99           true  sample              72.090          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999          true  sample             257.950          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999         true  sample             479.081          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00           true  sample            1509.949          ms/op
     *
     * 6 threads
     *   Benchmark                                       (contended)   Mode  Cnt     Score     Error   Units
     *   DefaultConnectionPoolBenchmark.connectionCycle        false  thrpt    8  2050.750 ±  66.466  ops/ms
     *   DefaultConnectionPoolBenchmark.connectionCycle         true  thrpt    8  1091.895 ± 130.521  ops/ms
     *
     *   Benchmark                                                               (contended)    Mode      Cnt   Score    Error  Units
     *   DefaultConnectionPoolBenchmark.connectionCycle                                false  sample  7376278   0.003 ±  0.001  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00          false  sample           ≈ 10⁻⁴           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50          false  sample           ≈ 10⁻³           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90          false  sample            0.018           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95          false  sample            0.020           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99          false  sample            0.023           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999         false  sample            0.057           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999        false  sample            0.103           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00          false  sample            2.900           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle                                 true  sample  7643063   0.006 ±  0.001  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00           true  sample           ≈ 10⁻⁴           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50           true  sample           ≈ 10⁻³           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90           true  sample            0.020           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95           true  sample            0.024           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99           true  sample            0.119           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999          true  sample            0.281           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999         true  sample            0.548           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00           true  sample            6.947           ms/op
     *
     * 4.5.0-snapshot-fair
     * 1000 threads
     *   Benchmark                                       (contended)   Mode  Cnt   Score   Error   Units
     *   DefaultConnectionPoolBenchmark.connectionCycle        false  thrpt    8  34.614 ± 0.554  ops/ms
     *   DefaultConnectionPoolBenchmark.connectionCycle         true  thrpt    8  34.563 ± 0.246  ops/ms
     *
     *   Benchmark                                                               (contended)    Mode      Cnt   Score   Error  Units
     *   DefaultConnectionPoolBenchmark.connectionCycle                                false  sample  2044729  29.338 ± 0.004  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00          false  sample           27.853          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50          false  sample           28.967          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90          false  sample           30.048          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95          false  sample           31.064          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99          false  sample           36.372          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999         false  sample           53.740          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999        false  sample           64.946          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00          false  sample           97.255          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle                                 true  sample  2051629  29.260 ± 0.004  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00           true  sample           27.754          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50           true  sample           28.770          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90           true  sample           30.474          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95           true  sample           32.047          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99           true  sample           35.848          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999          true  sample           56.820          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999         true  sample           63.963          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00           true  sample           78.512          ms/op
     *
     * 6 threads
     *   Benchmark                                       (contended)   Mode  Cnt    Score   Error   Units
     *   DefaultConnectionPoolBenchmark.connectionCycle        false  thrpt    8  248.959 ± 1.776  ops/ms
     *   DefaultConnectionPoolBenchmark.connectionCycle         true  thrpt    8  247.461 ± 3.921  ops/ms
     *
     *   Benchmark                                                               (contended)    Mode      Cnt   Score    Error  Units
     *   DefaultConnectionPoolBenchmark.connectionCycle                                false  sample  7391259   0.024 ±  0.001  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00          false  sample           ≈ 10⁻³           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50          false  sample            0.024           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90          false  sample            0.025           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95          false  sample            0.026           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99          false  sample            0.047           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999         false  sample            0.081           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999        false  sample            0.342           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00          false  sample            2.638           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle                                 true  sample  7436407   0.024 ±  0.001  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00           true  sample           ≈ 10⁻³           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50           true  sample            0.024           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90           true  sample            0.024           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95           true  sample            0.025           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99           true  sample            0.044           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999          true  sample            0.088           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999         true  sample            0.532           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00           true  sample            5.931           ms/op
     *
     * 4.2.0_JAVA-4435
     * 1000 threads
     *   Benchmark                                       (contended)   Mode  Cnt     Score     Error   Units
     *   DefaultConnectionPoolBenchmark.connectionCycle        false  thrpt    8  2694.740 ± 197.240  ops/ms
     *   DefaultConnectionPoolBenchmark.connectionCycle         true  thrpt    8    34.765 ±   0.419  ops/ms
     *
     *   Benchmark                                                               (contended)    Mode        Cnt     Score   Error  Units
     *   DefaultConnectionPoolBenchmark.connectionCycle                                false  sample  154711971     0.357 ± 0.004  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00          false  sample               ≈ 10⁻⁴          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50          false  sample                0.004          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90          false  sample                0.008          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95          false  sample                0.009          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99          false  sample                0.014          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999         false  sample                0.029          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999        false  sample              902.824          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00          false  sample             2516.582          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle                                 true  sample    2055286    29.200 ± 0.003  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00           true  sample               27.787          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50           true  sample               28.967          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90           true  sample               29.852          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95           true  sample               30.540          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99           true  sample               33.948          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999          true  sample               36.897          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999         true  sample               79.167          ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00           true  sample               81.002          ms/op
     *
     * 6 threads
     *   Benchmark                                       (contended)   Mode  Cnt     Score    Error   Units
     *   DefaultConnectionPoolBenchmark.connectionCycle        false  thrpt    8  2726.716 ± 32.036  ops/ms
     *   DefaultConnectionPoolBenchmark.connectionCycle         true  thrpt    8   230.279 ± 34.865  ops/ms
     *
     *   Benchmark                                                               (contended)    Mode       Cnt   Score    Error  Units
     *   DefaultConnectionPoolBenchmark.connectionCycle                                false  sample  10039976   0.002 ±  0.001  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00          false  sample            ≈ 10⁻⁴           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50          false  sample             0.002           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90          false  sample             0.004           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95          false  sample             0.005           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99          false  sample             0.006           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999         false  sample             0.011           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999        false  sample             0.022           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00          false  sample             6.119           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle                                 true  sample   7311157   0.025 ±  0.001  ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.00           true  sample            ≈ 10⁻⁴           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.50           true  sample             0.024           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.90           true  sample             0.025           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.95           true  sample             0.026           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.99           true  sample             0.044           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.999          true  sample             0.089           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p0.9999         true  sample             0.478           ms/op
     *   DefaultConnectionPoolBenchmark.connectionCycle:connectionCycle·p1.00           true  sample             4.735           ms/op
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
        @Param({"false", "true"})
        public boolean contended;
        DefaultConnectionPool pool;

        public BenchmarkPoolState() {
        }

        @Setup(Level.Trial)
        public final void setupTrial(BenchmarkParams params) {
            int threads = params.getThreads();
            int maxSize = contended ? threads / (threads == 6 ? 3 : 4) : threads;
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
