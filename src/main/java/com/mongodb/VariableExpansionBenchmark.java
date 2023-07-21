package com.mongodb;

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
import org.trimou.Mustache;
import org.trimou.engine.MustacheEngineBuilder;
import org.trimou.engine.locale.FixedLocaleSupport;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.openjdk.jmh.runner.options.TimeValue.seconds;

/**
 * Here I also compare against a dynamic template engine http://trimou.org,
 * but not against a statically compiled one like https://github.com/jstachio/jstachio.
 * See https://github.com/jstachio/jstachio#performance for performance comparison of template engines.
 *
 * The ranking in this benchmark is:
 * 1. template (`com.mongodb.kafka.connect.source.topic.mapping.DefaultTopicMapper.TopicNameTemplate`)
 *    About 1.9x-5.9x higher throughput than that of `java.util.Formatter`.
 *    About 17x-43x higher throughput than that of `java.lang.String.replaceAll`.
 * 2. formatter (`java.util.Formatter` with a manually prepared format)
 *    About 1x-1.7x higher throughput than that of `org.trimou.Mustache`.
 *    It loses to `TopicNameTemplate` because it has to locate format specifiers in the format each time it formats.
 *    There is no way to compile a format in advance.
 * 3. trimou (`org.trimou.Mustache` with a manually prepared template)
 * 4. stringReplaceAll (`java.lang.String.replaceAll` called dynamically for each expandable variable)
 *
 * Benchmark                                      (dataVariant)  (templateVariant)   Mode  Cnt   Score   Error   Units
 * VariableExpansionBenchmark.formatter                      DB              SHORT  thrpt    6   9.978 ± 0.629  ops/us
 * VariableExpansionBenchmark.formatter                      DB             MEDIUM  thrpt    6   6.784 ± 0.224  ops/us
 * VariableExpansionBenchmark.formatter                      DB               LONG  thrpt    6   2.730 ± 0.087  ops/us
 * VariableExpansionBenchmark.formatter                 DB_COLL              SHORT  thrpt    6   8.447 ± 0.330  ops/us
 * VariableExpansionBenchmark.formatter                 DB_COLL             MEDIUM  thrpt    6   5.962 ± 0.207  ops/us
 * VariableExpansionBenchmark.formatter                 DB_COLL               LONG  thrpt    6   2.256 ± 0.368  ops/us
 * VariableExpansionBenchmark.stringFormat                   DB              SHORT  thrpt    6   8.620 ± 0.249  ops/us
 * VariableExpansionBenchmark.stringFormat                   DB             MEDIUM  thrpt    6   5.684 ± 0.495  ops/us
 * VariableExpansionBenchmark.stringFormat                   DB               LONG  thrpt    6   2.453 ± 0.048  ops/us
 * VariableExpansionBenchmark.stringFormat              DB_COLL              SHORT  thrpt    6   7.584 ± 0.491  ops/us
 * VariableExpansionBenchmark.stringFormat              DB_COLL             MEDIUM  thrpt    6   5.268 ± 0.210  ops/us
 * VariableExpansionBenchmark.stringFormat              DB_COLL               LONG  thrpt    6   2.072 ± 0.165  ops/us
 * VariableExpansionBenchmark.stringReplaceAll               DB              SHORT  thrpt    6   0.645 ± 0.022  ops/us
 * VariableExpansionBenchmark.stringReplaceAll               DB             MEDIUM  thrpt    6   0.577 ± 0.012  ops/us
 * VariableExpansionBenchmark.stringReplaceAll               DB               LONG  thrpt    6   0.396 ± 0.026  ops/us
 * VariableExpansionBenchmark.stringReplaceAll          DB_COLL              SHORT  thrpt    6   0.609 ± 0.031  ops/us
 * VariableExpansionBenchmark.stringReplaceAll          DB_COLL             MEDIUM  thrpt    6   0.525 ± 0.016  ops/us
 * VariableExpansionBenchmark.stringReplaceAll          DB_COLL               LONG  thrpt    6   0.371 ± 0.008  ops/us
 * VariableExpansionBenchmark.template                       DB              SHORT  thrpt    6  28.184 ± 1.297  ops/us
 * VariableExpansionBenchmark.template                       DB             MEDIUM  thrpt    6  23.397 ± 0.632  ops/us
 * VariableExpansionBenchmark.template                       DB               LONG  thrpt    6  13.471 ± 0.614  ops/us
 * VariableExpansionBenchmark.template                  DB_COLL              SHORT  thrpt    6  16.133 ± 2.349  ops/us
 * VariableExpansionBenchmark.template                  DB_COLL             MEDIUM  thrpt    6  14.514 ± 0.635  ops/us
 * VariableExpansionBenchmark.template                  DB_COLL               LONG  thrpt    6   6.344 ± 0.358  ops/us
 * VariableExpansionBenchmark.trimou                         DB              SHORT  thrpt    6   6.234 ± 0.157  ops/us
 * VariableExpansionBenchmark.trimou                         DB             MEDIUM  thrpt    6   5.447 ± 0.228  ops/us
 * VariableExpansionBenchmark.trimou                         DB               LONG  thrpt    6   2.804 ± 0.097  ops/us
 * VariableExpansionBenchmark.trimou                    DB_COLL              SHORT  thrpt    6   4.855 ± 0.141  ops/us
 * VariableExpansionBenchmark.trimou                    DB_COLL             MEDIUM  thrpt    6   4.414 ± 0.187  ops/us
 * VariableExpansionBenchmark.trimou                    DB_COLL               LONG  thrpt    6   1.890 ± 0.062  ops/us
 * VariableExpansionBenchmark.uncompiledTemplate             DB              SHORT  thrpt    6   4.296 ± 0.112  ops/us
 * VariableExpansionBenchmark.uncompiledTemplate             DB             MEDIUM  thrpt    6   2.507 ± 0.067  ops/us
 * VariableExpansionBenchmark.uncompiledTemplate             DB               LONG  thrpt    6   1.204 ± 0.106  ops/us
 * VariableExpansionBenchmark.uncompiledTemplate        DB_COLL              SHORT  thrpt    6   3.511 ± 0.712  ops/us
 * VariableExpansionBenchmark.uncompiledTemplate        DB_COLL             MEDIUM  thrpt    6   2.249 ± 0.143  ops/us
 * VariableExpansionBenchmark.uncompiledTemplate        DB_COLL               LONG  thrpt    6   1.087 ± 0.023  ops/us
 */
public class VariableExpansionBenchmark {
    public VariableExpansionBenchmark() {
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
                .include(VariableExpansionBenchmark.class.getSimpleName() + "\\.*")
                .forks(2)
                .warmupForks(0)
                .warmupIterations(4)
                .warmupTime(seconds(1))
                .measurementIterations(3)
                .measurementTime(seconds(1));
        Collection<Integer> threadCounts = singletonList(1);
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
                .timeUnit(TimeUnit.MICROSECONDS));
    }

    @Benchmark
    public String template(final BenchmarkState state) {
        return state.templateComputer.compute(state.dbName, state.collName);
    }

    @Benchmark
    public String uncompiledTemplate(final BenchmarkState state) {
        return state.uncompiledTemplateComputer.compute(state.dbName, state.collName);
    }

    @Benchmark
    public String formatter(final BenchmarkState state) {
        return state.formatterComputer.compute(state.dbName, state.collName);
    }

    @Benchmark
    public String stringFormat(final BenchmarkState state) {
        return state.stringFormatComputer.compute(state.dbName, state.collName);
    }

    @Benchmark
    public String stringReplaceAll(final BenchmarkState state) {
        return state.stringReplaceAllComputer.compute(state.dbName, state.collName);
    }

    @Benchmark
    public String trimou(final BenchmarkState state) {
        return state.trimouComputerCreator.compute(state.dbName, state.collName);
    }

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Param({"DB", "DB_COLL"})
        public String dataVariant;
        @Param({"SHORT", "MEDIUM", "LONG"})
        public String templateVariant;
        public TemplateVariant.Computer templateComputer;
        public TemplateVariant.Computer uncompiledTemplateComputer;
        public TemplateVariant.Computer formatterComputer;
        public TemplateVariant.Computer stringFormatComputer;
        public TemplateVariant.Computer stringReplaceAllComputer;
        public TemplateVariant.Computer trimouComputerCreator;
        public String dbName;
        public String collName;

        public BenchmarkState() {
        }

        @Setup(Level.Trial)
        public final void setupTrial(BenchmarkParams params) {
            DataVariant parsedDataVariant = DataVariant.valueOf(dataVariant);
            dbName = parsedDataVariant.dbName;
            collName = parsedDataVariant.collName;
            String separator = parsedDataVariant.separator;
            TemplateVariant parsedTemplateVariant = TemplateVariant.valueOf(templateVariant);
            templateComputer = parsedTemplateVariant.topicNameTemplateComputerCreator.create(separator);
            uncompiledTemplateComputer = parsedTemplateVariant.uncompiledTopicNameTemplateComputerCreator.create(separator);
            formatterComputer = parsedTemplateVariant.formatterComputerCreator.create(separator);
            stringFormatComputer = parsedTemplateVariant.stringFormatComputerCreator.create(separator);
            stringReplaceAllComputer = parsedTemplateVariant.stringReplaceAllComputerCreator.create(separator);
            trimouComputerCreator = parsedTemplateVariant.trimouComputerCreator.create(separator);
            String templateComputerResult = templateComputer.compute(dbName, collName);
            String uncompiledTemplateComputerResult = uncompiledTemplateComputer.compute(dbName, collName);
            String formatterComputerResult = formatterComputer.compute(dbName, collName);
            String stringFormatComputerResult = stringFormatComputer.compute(dbName, collName);
            String stringReplaceAllComputerResult = stringReplaceAllComputer.compute(dbName, collName);
            String trimouComputerCreatorResult = trimouComputerCreator.compute(dbName, collName);
            if (!templateComputerResult.equals(uncompiledTemplateComputerResult)) {
                throw new AssertionError(format("%n%s%n%s", templateComputerResult, uncompiledTemplateComputerResult));
            }
            if (!templateComputerResult.equals(formatterComputerResult)) {
                throw new AssertionError(format("%n%s%n%s", templateComputerResult, formatterComputerResult));
            }
            if (!templateComputerResult.equals(stringFormatComputerResult)) {
                throw new AssertionError(format("%n%s%n%s", templateComputerResult, stringFormatComputerResult));
            }
            if (!templateComputerResult.equals(stringReplaceAllComputerResult)) {
                throw new AssertionError(format("%n%s%n%s", templateComputerResult, stringReplaceAllComputerResult));
            }
            if (!templateComputerResult.equals(trimouComputerCreatorResult)) {
                throw new AssertionError(format("%n%s%n%s", templateComputerResult, trimouComputerCreatorResult));
            }
        }

        enum TemplateVariant {
            SHORT(
                    sep -> {
                        TopicNameTemplate template = new TopicNameTemplate("{db}{sep_coll}", sep);
                        return template::compute;
                    },
                    sep -> (db, coll) -> new TopicNameTemplate("{db}{sep_coll}", sep).compute(db, coll),
                    sep -> {
                        StringBuilder builder = new StringBuilder();
                        Formatter formatter = new Formatter(builder, Locale.ROOT);
                        return (db, coll) -> {
                            String sep_coll = TopicNameTemplate.VarName.SEP_COLL.computeValue(db, coll, sep);
                            builder.setLength(0);
                            formatter.format("%s%s", db, sep_coll);
                            return builder.toString();
                        };
                    },
                    sep -> (db, coll) -> {
                        String sep_coll = TopicNameTemplate.VarName.SEP_COLL.computeValue(db, coll, sep);
                        return String.format(Locale.ROOT, "%s%s", db, sep_coll);
                    },
                    sep -> (db, coll) -> computeReplaceAll("{db}{sep_coll}", db, coll, sep),
                    sep -> {
                        StringBuilder builder = new StringBuilder();
                        Mustache mustacheTemplate =
                                MustacheEngineBuilder.newBuilder().setLocaleSupport(FixedLocaleSupport.from(Locale.ROOT)).build()
                                .compileMustache("{{db}}{{sep_coll}}");
                        HashMap<String, String> context = HashMap.newHashMap(2);
                        return (db, coll) -> {
                            context.put("db", db);
                            context.put("sep_coll", TopicNameTemplate.VarName.SEP_COLL.computeValue(db, coll, sep));
                            builder.setLength(0);
                            mustacheTemplate.render(builder, context);
                            return builder.toString();
                        };
                    }
            ),
            MEDIUM(
                    sep -> {
                        TopicNameTemplate template = new TopicNameTemplate("prefix{sep}{db}{sep_coll}{sep}postfix", sep);
                        return template::compute;
                    },
                    sep -> (db, coll) -> new TopicNameTemplate("prefix{sep}{db}{sep_coll}{sep}postfix", sep).compute(db, coll),
                    sep -> {
                        String format = "prefix" + sep + "%s%s" + sep + "postfix";
                        StringBuilder builder = new StringBuilder();
                        Formatter formatter = new Formatter(builder, Locale.ROOT);
                        return (db, coll) -> {
                            String sep_coll = TopicNameTemplate.VarName.SEP_COLL.computeValue(db, coll, sep);
                            builder.setLength(0);
                            formatter.format(format, db, sep_coll);
                            return builder.toString();
                        };
                    },
                    sep -> {
                        String format = "prefix" + sep + "%s%s" + sep + "postfix";
                        return (db, coll) -> {
                            String sep_coll = TopicNameTemplate.VarName.SEP_COLL.computeValue(db, coll, sep);
                            return String.format(Locale.ROOT, format, db, sep_coll);
                        };
                    },
                    sep -> (db, coll) -> computeReplaceAll("prefix{sep}{db}{sep_coll}{sep}postfix", db, coll, sep),
                    sep -> {
                        StringBuilder builder = new StringBuilder();
                        Mustache mustacheTemplate =
                                MustacheEngineBuilder.newBuilder().setLocaleSupport(FixedLocaleSupport.from(Locale.ROOT)).build()
                                .compileMustache("prefix" + sep + "{{db}}{{sep_coll}}" + sep + "postfix");
                        HashMap<String, String> context = HashMap.newHashMap(2);
                        return (db, coll) -> {
                            context.put("db", db);
                            context.put("sep_coll", TopicNameTemplate.VarName.SEP_COLL.computeValue(db, coll, sep));
                            builder.setLength(0);
                            mustacheTemplate.render(builder, context);
                            return builder.toString();
                        };
                    }
            ),
            LONG(
                    sep -> {
                        TopicNameTemplate template = new TopicNameTemplate(
                                "qieoruyajhdsgfzjzk{coll}jadhfgajkdhsgkf{db}{sep}{coll}sjdfgdk{sep_coll_sep}aerawuyfzkjvbzhv{sep}{coll_sep}sjfdghsj",
                                sep);
                        return template::compute;
                    },
                    sep -> (db, coll) -> new TopicNameTemplate(
                                "qieoruyajhdsgfzjzk{coll}jadhfgajkdhsgkf{db}{sep}{coll}sjdfgdk{sep_coll_sep}aerawuyfzkjvbzhv{sep}{coll_sep}sjfdghsj",
                                sep).compute(db, coll),
                    sep -> {
                        String format = "qieoruyajhdsgfzjzk%sjadhfgajkdhsgkf%s" + sep + "%ssjdfgdk%saerawuyfzkjvbzhv" + sep + "%ssjfdghsj";
                        StringBuilder builder = new StringBuilder();
                        Formatter formatter = new Formatter(builder, Locale.ROOT);
                        return (db, coll) -> {
                            String coll_sep = TopicNameTemplate.VarName.COLL_SEP.computeValue(db, coll, sep);
                            String sep_coll_sep = TopicNameTemplate.VarName.SEP_COLL_SEP.computeValue(db, coll, sep);
                            builder.setLength(0);
                            formatter.format(format, coll, db, coll, sep_coll_sep, coll_sep);
                            return builder.toString();
                        };
                    },
                    sep -> {
                        String format = "qieoruyajhdsgfzjzk%sjadhfgajkdhsgkf%s" + sep + "%ssjdfgdk%saerawuyfzkjvbzhv" + sep + "%ssjfdghsj";
                        return (db, coll) -> {
                            String coll_sep = TopicNameTemplate.VarName.COLL_SEP.computeValue(db, coll, sep);
                            String sep_coll_sep = TopicNameTemplate.VarName.SEP_COLL_SEP.computeValue(db, coll, sep);
                            return String.format(Locale.ROOT, format, coll, db, coll, sep_coll_sep, coll_sep);
                        };
                    },
                    sep -> (db, coll) -> computeReplaceAll(
                            "qieoruyajhdsgfzjzk{coll}jadhfgajkdhsgkf{db}{sep}{coll}sjdfgdk{sep_coll_sep}aerawuyfzkjvbzhv{sep}{coll_sep}sjfdghsj",
                            db, coll, sep),
                    sep -> {
                        StringBuilder builder = new StringBuilder();
                        Mustache mustacheTemplate =
                                MustacheEngineBuilder.newBuilder().setLocaleSupport(FixedLocaleSupport.from(Locale.ROOT)).build()
                                .compileMustache("qieoruyajhdsgfzjzk{{coll}}jadhfgajkdhsgkf{{db}}" + sep + "{{coll}}sjdfgdk" + "{{sep_coll_sep}}aerawuyfzkjvbzhv" + sep + "{{coll_sep}}sjfdghsj");
                        HashMap<String, String> context = HashMap.newHashMap(4);
                        return (db, coll) -> {
                            context.put("db", db);
                            context.put("coll", coll);
                            context.put("coll_sep", TopicNameTemplate.VarName.COLL_SEP.computeValue(db, coll, sep));
                            context.put("sep_coll_sep", TopicNameTemplate.VarName.SEP_COLL_SEP.computeValue(db, coll, sep));
                            builder.setLength(0);
                            mustacheTemplate.render(builder, context);
                            return builder.toString();
                        };
                    }
            );

            final ComputerCreator topicNameTemplateComputerCreator;
            final ComputerCreator uncompiledTopicNameTemplateComputerCreator;
            final ComputerCreator formatterComputerCreator;
            final ComputerCreator stringFormatComputerCreator;
            final ComputerCreator stringReplaceAllComputerCreator;
            final ComputerCreator trimouComputerCreator;

            TemplateVariant(
                    final ComputerCreator topicNameTemplateComputerCreator,
                    final ComputerCreator uncompiledTopicNameTemplateComputerCreator,
                    final ComputerCreator formatterComputerCreator,
                    final ComputerCreator stringFormatComputerCreator,
                    final ComputerCreator stringReplaceAllComputerCreator,
                    final ComputerCreator trimouComputerCreator) {
                this.topicNameTemplateComputerCreator = topicNameTemplateComputerCreator;
                this.uncompiledTopicNameTemplateComputerCreator = uncompiledTopicNameTemplateComputerCreator;
                this.formatterComputerCreator = formatterComputerCreator;
                this.stringFormatComputerCreator = stringFormatComputerCreator;
                this.stringReplaceAllComputerCreator = stringReplaceAllComputerCreator;
                this.trimouComputerCreator = trimouComputerCreator;
            }

            private static String computeReplaceAll(String template, final String dbName, final String collName, final String separator) {
                for (TopicNameTemplate.VarName varName : TopicNameTemplate.VarName.values()) {
                    template = template.replaceAll(varName.placeholderRegex(), varName.computeValue(dbName, collName, separator));
                }
                return template;
            }

            @FunctionalInterface
            private interface ComputerCreator {
                Computer create(final String separator);
            }

            @FunctionalInterface
            private interface Computer {
                String compute(String dbName, String collName);
            }
        }

        enum DataVariant {
            DB("myDb", "", "."),
            DB_COLL("myDb", "myColl", ".");

            final String dbName;
            final String collName;
            final String separator;

            DataVariant(final String dbName, final String collName, final String separator) {
                this.dbName = dbName;
                this.collName = collName;
                this.separator = separator;
            }
        }
    }

    private static final class ConnectConfigException extends RuntimeException {
        ConnectConfigException(final String msg) {
            super(msg);
        }
    }

    @FunctionalInterface
    private interface ConfigExceptionSupplier extends Function<String, ConnectConfigException> {}

    private static final class TopicNameTemplate {
        private static final char START_VAR_NAME = '{';
        private static final char END_VAR_NAME = '}';

        private final String compiledTemplate;
        private final ArrayList<Map.Entry<Integer, VarName>> varExpansions;
        private final String separator;
        private final StringBuilder builder;

        TopicNameTemplate(
                final String template,
                final String separator)
                throws ConnectConfigException {
            ConfigExceptionSupplier configExceptionSupplier = ConnectConfigException::new;
            this.builder = new StringBuilder(template.length());
            varExpansions = new ArrayList<>();
            compiledTemplate = compile(template, separator, builder, varExpansions, configExceptionSupplier);
            this.separator = separator;
        }

        String compute(final String dbName, final String collName) {
            if(isEmpty()) {
                throw new AssertionError();
            }
            if (varExpansions.isEmpty()) {
                return compiledTemplate;
            }
            builder.setLength(0);
            builder.append(compiledTemplate);
            // As we expand variables, the index of the following expansions offsets correspondingly,
            // we use `varExpansionIdxOffset` to track this offset.
            int varExpansionIdxOffset = 0;
            for (Map.Entry<Integer, VarName> varExpansion : varExpansions) {
                int idx = varExpansion.getKey();
                VarName varName = varExpansion.getValue();
                final String varValue = varName.computeValue(dbName, collName, separator);
                if (!varValue.isEmpty()) {
                    builder.insert(idx + varExpansionIdxOffset, varValue);
                    varExpansionIdxOffset += varValue.length();
                }
            }
            return builder.toString();
        }

        boolean isEmpty() {
            return compiledTemplate.isEmpty() && varExpansions.isEmpty();
        }

        @Override
        public String toString() {
            return "TopicNameTemplate{"
                    + "compiledTemplate='"
                    + compiledTemplate
                    + '\''
                    + ", varExpansions="
                    + varExpansions
                    + ", separator='"
                    + separator
                    + '\''
                    + '}';
        }

        private static String compile(
                final String template,
                final String separator,
                final StringBuilder compiledTemplateBuilder,
                final ArrayList<Map.Entry<Integer, VarName>> varExpansions,
                final ConfigExceptionSupplier configExceptionSupplier)
                throws ConnectConfigException {
            String varExpansionErrorMessageFormat =
                    "Variable expansion syntax is violated, unexpected '%c'";
            int balance = 0;
            int firstUncompiledIdx = 0;
            int varPlaceholderStartIdx = -1;
            // The index of a variable expansion in `compiledTemplateBuilder`
            // differs from the index of the variable placeholder in `template`,
            // because a compiled template is different from the raw template:
            // - it does not have variable placeholders;
            // - it has the `VarName.SEP` variable expanded.
            // We use `varExpansionIdxOffset` to track the difference.
            int varExpansionIdxOffset = 0;
            char[] chars = template.toCharArray();
            for (int i = 0; i < chars.length; i++) {
                char c = chars[i];
                switch (c) {
                    case START_VAR_NAME:
                        balance += 1;
                        throwIfInvalidBalance(
                                balance, 0, 1, varExpansionErrorMessageFormat, configExceptionSupplier);
                        varPlaceholderStartIdx = i;
                        break;
                    case END_VAR_NAME:
                        balance -= 1;
                        throwIfInvalidBalance(
                                balance, 0, 1, varExpansionErrorMessageFormat, configExceptionSupplier);
                        int varPlaceholderLength = i - varPlaceholderStartIdx + 1;
                        VarName varName =
                                VarName.of(
                                        template.substring(varPlaceholderStartIdx + 1, i), configExceptionSupplier);
                        compiledTemplateBuilder.append(template, firstUncompiledIdx, varPlaceholderStartIdx);
                        if (varName == VarName.SEP) {
                            compiledTemplateBuilder.append(separator);
                            varExpansionIdxOffset -= separator.length();
                        } else {
                            varExpansions.add(
                                    new AbstractMap.SimpleImmutableEntry<>(
                                            varPlaceholderStartIdx - varExpansionIdxOffset, varName));
                        }
                        varExpansionIdxOffset += varPlaceholderLength;
                        firstUncompiledIdx = i + 1;
                        break;
                    default:
                        // nothing to do
                }
            }
            throwIfInvalidBalance(balance, 0, 0, varExpansionErrorMessageFormat, configExceptionSupplier);
            compiledTemplateBuilder.append(template, firstUncompiledIdx, template.length());
            return compiledTemplateBuilder.toString();
        }

        private static void throwIfInvalidBalance(
                final int balance,
                final int minBalance,
                final int maxBalance,
                final String errorMessageFormat,
                final ConfigExceptionSupplier configExceptionSupplier)
                throws ConnectConfigException {
            if (balance < minBalance) {
                throw configExceptionSupplier.apply(format(Locale.ROOT, errorMessageFormat, END_VAR_NAME));
            } else if (balance > maxBalance) {
                throw configExceptionSupplier.apply(
                        format(Locale.ROOT, errorMessageFormat, START_VAR_NAME));
            }
        }

        private enum VarName {
            DB(placeholderRegex("db"),
                    (dbName, collName, separator) -> dbName),
            SEP(placeholderRegex("sep"),
                    (dbName, collName, separator) -> separator),
            COLL(placeholderRegex("coll"),
                    (dbName, collName, separator) -> collName),
            SEP_COLL(placeholderRegex("sep_coll"),
                    (dbName, collName, separator) -> collName.isEmpty() ? "" : separator + collName),
            COLL_SEP(placeholderRegex("coll_sep"),
                    (dbName, collName, separator) -> collName.isEmpty() ? "" : collName + separator),
            SEP_COLL_SEP(placeholderRegex("sep_coll_sep"),
                    (dbName, collName, separator) -> collName.isEmpty() ? "" : separator + collName + separator);

            private static final Set<String> SUPPORTED_VAR_NAMES =
                    Stream.of(VarName.values())
                            .map(VarName::name)
                            .map(varName -> varName.toLowerCase(Locale.ROOT))
                            .collect(Collectors.toSet());

            private final String placeholder;
            private final ValueComputer valueComputer;

            VarName(final String placeholder, final ValueComputer computer) {
                this.placeholder = placeholder;
                this.valueComputer = computer;
            }

            String placeholderRegex() {
                return placeholder;
            }

            String computeValue(final String dbName, final String collName, final String separator) {
                return this.valueComputer.compute(dbName, collName, separator);
            }

            static VarName of(final String s, final ConfigExceptionSupplier configExceptionSupplier)
                    throws ConnectConfigException {
                if (!SUPPORTED_VAR_NAMES.contains(s)) {
                    // We do this to enforce case-sensitivity,
                    // otherwise we could have called `VarName.valueOf`
                    // and checked for `IllegalArgumentException`s.
                    throw configExceptionSupplier.apply(s + " is not a supported variable name");
                }
                return VarName.valueOf(s.toUpperCase(Locale.ROOT));
            }

            private static String placeholderRegex(final String varName) {
                return "\\" + START_VAR_NAME + varName + "\\" + END_VAR_NAME;
            }

            @FunctionalInterface
            private interface ValueComputer {
                String compute(String dbName, String collName, String separator);
            }
        }
    }
}
