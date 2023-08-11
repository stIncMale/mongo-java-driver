package com.mongodb;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static org.openjdk.jmh.runner.options.TimeValue.seconds;

/**
 * 6 threads
 * Benchmark                    (extraWork)   Mode  Cnt   Score   Error   Units
 * LockBenchmark.reentrantLock      NOTHING  thrpt   33  43.509 ± 0.894  ops/us
 * LockBenchmark.reentrantLock  CONSUME_CPU  thrpt   33  29.218 ± 0.574  ops/us
 * LockBenchmark.stampedLock        NOTHING  thrpt   33  47.259 ± 0.829  ops/us
 * LockBenchmark.stampedLock    CONSUME_CPU  thrpt   33  26.550 ± 2.773  ops/us
 * LockBenchmark.synchr0nized       NOTHING  thrpt   33   9.165 ± 0.903  ops/us
 * LockBenchmark.synchr0nized   CONSUME_CPU  thrpt   33  11.198 ± 1.098  ops/us
 *
 * Benchmark                            (extraWork)    Mode      Cnt     Score   Error  Units
 * LockBenchmark.reentrantLock              NOTHING  sample  6602005     1.153 ± 0.007  us/op
 * LockBenchmark.reentrantLock:p0.00        NOTHING  sample              0.001          us/op
 * LockBenchmark.reentrantLock:p0.50        NOTHING  sample              0.052          us/op
 * LockBenchmark.reentrantLock:p0.90        NOTHING  sample              0.103          us/op
 * LockBenchmark.reentrantLock:p0.95        NOTHING  sample              0.233          us/op
 * LockBenchmark.reentrantLock:p0.99        NOTHING  sample             32.416          us/op
 * LockBenchmark.reentrantLock:p0.999       NOTHING  sample             50.368          us/op
 * LockBenchmark.reentrantLock:p0.9999      NOTHING  sample             78.208          us/op
 * LockBenchmark.reentrantLock:p1.00        NOTHING  sample            226.816          us/op
 * LockBenchmark.reentrantLock          CONSUME_CPU  sample  5636334     1.422 ± 0.010  us/op
 * LockBenchmark.reentrantLock:p0.00    CONSUME_CPU  sample              0.001          us/op
 * LockBenchmark.reentrantLock:p0.50    CONSUME_CPU  sample              0.061          us/op
 * LockBenchmark.reentrantLock:p0.90    CONSUME_CPU  sample              0.115          us/op
 * LockBenchmark.reentrantLock:p0.95    CONSUME_CPU  sample              0.450          us/op
 * LockBenchmark.reentrantLock:p0.99    CONSUME_CPU  sample             38.400          us/op
 * LockBenchmark.reentrantLock:p0.999   CONSUME_CPU  sample             58.560          us/op
 * LockBenchmark.reentrantLock:p0.9999  CONSUME_CPU  sample             80.128          us/op
 * LockBenchmark.reentrantLock:p1.00    CONSUME_CPU  sample           3145.728          us/op
 * LockBenchmark.stampedLock                NOTHING  sample  6873868     1.469 ± 0.010  us/op
 * LockBenchmark.stampedLock:p0.00          NOTHING  sample              0.001          us/op
 * LockBenchmark.stampedLock:p0.50          NOTHING  sample              0.053          us/op
 * LockBenchmark.stampedLock:p0.90          NOTHING  sample              0.101          us/op
 * LockBenchmark.stampedLock:p0.95          NOTHING  sample              0.188          us/op
 * LockBenchmark.stampedLock:p0.99          NOTHING  sample             41.792          us/op
 * LockBenchmark.stampedLock:p0.999         NOTHING  sample             81.280          us/op
 * LockBenchmark.stampedLock:p0.9999        NOTHING  sample            110.926          us/op
 * LockBenchmark.stampedLock:p1.00          NOTHING  sample           2912.256          us/op
 * LockBenchmark.stampedLock            CONSUME_CPU  sample  4925497     3.917 ± 0.024  us/op
 * LockBenchmark.stampedLock:p0.00      CONSUME_CPU  sample              0.001          us/op
 * LockBenchmark.stampedLock:p0.50      CONSUME_CPU  sample              0.060          us/op
 * LockBenchmark.stampedLock:p0.90      CONSUME_CPU  sample              0.144          us/op
 * LockBenchmark.stampedLock:p0.95      CONSUME_CPU  sample             36.992          us/op
 * LockBenchmark.stampedLock:p0.99      CONSUME_CPU  sample             80.768          us/op
 * LockBenchmark.stampedLock:p0.999     CONSUME_CPU  sample            120.704          us/op
 * LockBenchmark.stampedLock:p0.9999    CONSUME_CPU  sample            155.648          us/op
 * LockBenchmark.stampedLock:p1.00      CONSUME_CPU  sample           3432.448          us/op
 * LockBenchmark.synchr0nized               NOTHING  sample  6087409     2.005 ± 0.009  us/op
 * LockBenchmark.synchr0nized:p0.00         NOTHING  sample              0.001          us/op
 * LockBenchmark.synchr0nized:p0.50         NOTHING  sample              0.474          us/op
 * LockBenchmark.synchr0nized:p0.90         NOTHING  sample              3.196          us/op
 * LockBenchmark.synchr0nized:p0.95         NOTHING  sample              6.440          us/op
 * LockBenchmark.synchr0nized:p0.99         NOTHING  sample             32.672          us/op
 * LockBenchmark.synchr0nized:p0.999        NOTHING  sample             61.696          us/op
 * LockBenchmark.synchr0nized:p0.9999       NOTHING  sample             97.536          us/op
 * LockBenchmark.synchr0nized:p1.00         NOTHING  sample           3108.864          us/op
 * LockBenchmark.synchr0nized           CONSUME_CPU  sample  6709846     2.565 ± 0.011  us/op
 * LockBenchmark.synchr0nized:p0.00     CONSUME_CPU  sample              0.001          us/op
 * LockBenchmark.synchr0nized:p0.50     CONSUME_CPU  sample              0.420          us/op
 * LockBenchmark.synchr0nized:p0.90     CONSUME_CPU  sample              3.356          us/op
 * LockBenchmark.synchr0nized:p0.95     CONSUME_CPU  sample             11.600          us/op
 * LockBenchmark.synchr0nized:p0.99     CONSUME_CPU  sample             47.552          us/op
 * LockBenchmark.synchr0nized:p0.999    CONSUME_CPU  sample             78.720          us/op
 * LockBenchmark.synchr0nized:p0.9999   CONSUME_CPU  sample            116.098          us/op
 * LockBenchmark.synchr0nized:p1.00     CONSUME_CPU  sample            937.984          us/op
 *
 * 60 threads
 * Benchmark                    (extraWork)   Mode  Cnt   Score   Error   Units
 * LockBenchmark.reentrantLock      NOTHING  thrpt   33  41.901 ± 0.733  ops/us
 * LockBenchmark.reentrantLock  CONSUME_CPU  thrpt   33  27.367 ± 1.423  ops/us
 * LockBenchmark.stampedLock        NOTHING  thrpt   33  44.238 ± 1.312  ops/us
 * LockBenchmark.stampedLock    CONSUME_CPU  thrpt   33  24.929 ± 1.981  ops/us
 * LockBenchmark.synchr0nized       NOTHING  thrpt   33  11.233 ± 1.545  ops/us
 * LockBenchmark.synchr0nized   CONSUME_CPU  thrpt   33  12.317 ± 1.255  ops/us
 *
 * Benchmark                            (extraWork)    Mode       Cnt      Score   Error  Units
 * LockBenchmark.reentrantLock              NOTHING  sample  49412805      7.228 ± 0.023  us/op
 * LockBenchmark.reentrantLock:p0.00        NOTHING  sample                0.001          us/op
 * LockBenchmark.reentrantLock:p0.50        NOTHING  sample                0.051          us/op
 * LockBenchmark.reentrantLock:p0.90        NOTHING  sample                0.097          us/op
 * LockBenchmark.reentrantLock:p0.95        NOTHING  sample                0.164          us/op
 * LockBenchmark.reentrantLock:p0.99        NOTHING  sample              326.656          us/op
 * LockBenchmark.reentrantLock:p0.999       NOTHING  sample              398.336          us/op
 * LockBenchmark.reentrantLock:p0.9999      NOTHING  sample              527.360          us/op
 * LockBenchmark.reentrantLock:p1.00        NOTHING  sample             2498.560          us/op
 * LockBenchmark.reentrantLock          CONSUME_CPU  sample  40674012      7.242 ± 0.027  us/op
 * LockBenchmark.reentrantLock:p0.00    CONSUME_CPU  sample                0.001          us/op
 * LockBenchmark.reentrantLock:p0.50    CONSUME_CPU  sample                0.060          us/op
 * LockBenchmark.reentrantLock:p0.90    CONSUME_CPU  sample                0.108          us/op
 * LockBenchmark.reentrantLock:p0.95    CONSUME_CPU  sample                0.178          us/op
 * LockBenchmark.reentrantLock:p0.99    CONSUME_CPU  sample              340.992          us/op
 * LockBenchmark.reentrantLock:p0.999   CONSUME_CPU  sample              425.472          us/op
 * LockBenchmark.reentrantLock:p0.9999  CONSUME_CPU  sample              640.000          us/op
 * LockBenchmark.reentrantLock:p1.00    CONSUME_CPU  sample             7979.008          us/op
 * LockBenchmark.stampedLock                NOTHING  sample  48914496      6.828 ± 0.023  us/op
 * LockBenchmark.stampedLock:p0.00          NOTHING  sample                0.001          us/op
 * LockBenchmark.stampedLock:p0.50          NOTHING  sample                0.051          us/op
 * LockBenchmark.stampedLock:p0.90          NOTHING  sample                0.085          us/op
 * LockBenchmark.stampedLock:p0.95          NOTHING  sample                0.146          us/op
 * LockBenchmark.stampedLock:p0.99          NOTHING  sample              308.736          us/op
 * LockBenchmark.stampedLock:p0.999         NOTHING  sample              379.392          us/op
 * LockBenchmark.stampedLock:p0.9999        NOTHING  sample              531.456          us/op
 * LockBenchmark.stampedLock:p1.00          NOTHING  sample             9650.176          us/op
 * LockBenchmark.stampedLock            CONSUME_CPU  sample  40706581      7.543 ± 0.026  us/op
 * LockBenchmark.stampedLock:p0.00      CONSUME_CPU  sample                0.001          us/op
 * LockBenchmark.stampedLock:p0.50      CONSUME_CPU  sample                0.059          us/op
 * LockBenchmark.stampedLock:p0.90      CONSUME_CPU  sample                0.111          us/op
 * LockBenchmark.stampedLock:p0.95      CONSUME_CPU  sample                0.176          us/op
 * LockBenchmark.stampedLock:p0.99      CONSUME_CPU  sample              324.608          us/op
 * LockBenchmark.stampedLock:p0.999     CONSUME_CPU  sample              423.936          us/op
 * LockBenchmark.stampedLock:p0.9999    CONSUME_CPU  sample              566.272          us/op
 * LockBenchmark.stampedLock:p1.00      CONSUME_CPU  sample             7823.360          us/op
 * LockBenchmark.synchr0nized               NOTHING  sample  42270556     20.633 ± 0.065  us/op
 * LockBenchmark.synchr0nized:p0.00         NOTHING  sample                0.001          us/op
 * LockBenchmark.synchr0nized:p0.50         NOTHING  sample                0.444          us/op
 * LockBenchmark.synchr0nized:p0.90         NOTHING  sample                2.832          us/op
 * LockBenchmark.synchr0nized:p0.95         NOTHING  sample                5.024          us/op
 * LockBenchmark.synchr0nized:p0.99         NOTHING  sample              792.576          us/op
 * LockBenchmark.synchr0nized:p0.999        NOTHING  sample             1218.560          us/op
 * LockBenchmark.synchr0nized:p0.9999       NOTHING  sample             1540.096          us/op
 * LockBenchmark.synchr0nized:p1.00         NOTHING  sample            21102.592          us/op
 * LockBenchmark.synchr0nized           CONSUME_CPU  sample  39579111     23.468 ± 0.083  us/op
 * LockBenchmark.synchr0nized:p0.00     CONSUME_CPU  sample                0.001          us/op
 * LockBenchmark.synchr0nized:p0.50     CONSUME_CPU  sample                0.456          us/op
 * LockBenchmark.synchr0nized:p0.90     CONSUME_CPU  sample                2.908          us/op
 * LockBenchmark.synchr0nized:p0.95     CONSUME_CPU  sample                5.240          us/op
 * LockBenchmark.synchr0nized:p0.99     CONSUME_CPU  sample              884.736          us/op
 * LockBenchmark.synchr0nized:p0.999    CONSUME_CPU  sample             1529.856          us/op
 * LockBenchmark.synchr0nized:p0.9999   CONSUME_CPU  sample             2506.752          us/op
 * LockBenchmark.synchr0nized:p1.00     CONSUME_CPU  sample            20578.304          us/op
 */
public class LockBenchmark {
    public LockBenchmark() {
    }

    /**
     * Run:
     * <pre>{@code
     * > mvn clean verify
     * > java -cp target/benchmarks.jar com.mongodb.LockBenchmark
     * }</pre>
     */
    public static void main(String[] args) {
        Supplier<ChainedOptionsBuilder> commonOpts = () -> new OptionsBuilder()
                .shouldFailOnError(true)
                .timeout(seconds(100))
                .jvmArgsAppend(
                        "-server"
                )
                .include(LockBenchmark.class.getSimpleName() + "\\.*")
                .syncIterations(true)
                .forks(11)
                .warmupForks(0)
                .warmupIterations(6)
                .warmupTime(seconds(1))
                .measurementIterations(3)
                .measurementTime(seconds(1));
        Collection<Integer> threadCounts = asList(
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
                .mode(Mode.SampleTime)
                .timeUnit(TimeUnit.MICROSECONDS));
    }

    @Benchmark
    public int synchr0nized(final BenchmarkState state) {
        synchronized (state.monitorHolder) {
            return work(state);
        }
    }

    @Benchmark
    public int reentrantLock(final BenchmarkState state) {
        Lock lock = state.reentrantLock;
        lock.lock();
        try {
            return work(state);
        } finally {
            lock.unlock();
        }
    }

    @Benchmark
    public int stampedLock(final BenchmarkState state) {
        StampedLock lock = state.stampedLock;
        long stamp = lock.writeLock();
        try {
            return work(state);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    private static int work(final BenchmarkState state) {
        state.extraWork.run();
        return state.counter++;
    }

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Param({
                "NOTHING",
                "CONSUME_CPU"
        })
        ExtraWork extraWork;
        Object monitorHolder;
        Lock reentrantLock;
        StampedLock stampedLock;
        IntSupplier work;
        private int counter;

        public BenchmarkState() {
        }

        @Setup(Level.Trial)
        public final void setupTrial(BenchmarkParams params) {
            counter = 0;
            monitorHolder = new Object();
            reentrantLock = new ReentrantLock(false);
            stampedLock = new StampedLock();
        }

        public enum ExtraWork implements Runnable {
            NOTHING(() -> {}),
            CONSUME_CPU(() -> Blackhole.consumeCPU(10));

            private final Runnable action;

            ExtraWork(final Runnable action) {
                this.action = action;
            }

            @Override
            public void run() {
                action.run();
            }
        }
    }
}
