package org.mp.naumann;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.reporter.GoogleSheetsReporter;
import org.mp.naumann.reporter.Reporter;
import org.mp.naumann.testcases.InitialAndIncrementalOneBatch;
import org.mp.naumann.testcases.TestCase;

import java.io.IOException;
import java.util.logging.Level;

public class BenchmarksApplication {

    @Parameter(names = "--name", required = true)
    private String name;
    @Parameter(names = "--help", help = true)
    private boolean help = false;
    @Parameter(names = "--spreadsheet")
    private String spreadsheet = "1ATQM5p6usBFImtxrn1yti-cjAtlnuli8kM4-ZpgrwfY";
    @Parameter(names = "--batchSize")
    private int batchSize = 100;
    @Parameter(names = "--splitLine")
    private int splitLine = 15000;
    @Parameter(names = "--dataSet")
    private String dataSet = "benchmark.adultfull.csv";
    @Parameter(names = "--sampling", arity = 1)
    private Boolean useSampling;
    @Parameter(names = "--clusterPruning", arity = 1)
    private Boolean useClusterPruning;
    @Parameter(names = "--innerClusterPruning", arity = 1)
    private Boolean useInnerClusterPruning;
    @Parameter(names = "--enhancedClusterPruning", arity = 1)
    private Boolean useEnhancedClusterPruning;
    @Parameter(names = "--recomputeDataStructures", arity = 1)
    private Boolean recomputeDataStructures;


    public static void main(String[] args) throws IOException {
        BenchmarksApplication app = new BenchmarksApplication();
        JCommander jc = new JCommander(app, args);
        if (app.help) {
            jc.usage();
            System.exit(0);
        }
        app.run();
    }

    public void run() throws IOException {
        int stopAfter = batchSize < 10 ? 100 : -1;

        FDLogger.setLevel(Level.OFF);
        setUp();

        IncrementalFDConfiguration config = IncrementalFDConfiguration.getVersion(name);
        if (useSampling != null) {
            config.setSampling(useSampling);
        }
        if (useClusterPruning != null) {
            config.setClusterPruning(useClusterPruning);
        }
        if (useInnerClusterPruning != null) {
            config.setInnerClusterPruning(useInnerClusterPruning);
        }
        if (useEnhancedClusterPruning != null) {
            config.setEnhancedClusterPruning(useEnhancedClusterPruning);
        }
        if (recomputeDataStructures != null) {
            config.setRecomputeDataStructures(recomputeDataStructures);
        }

        try {
            TestCase t = new InitialAndIncrementalOneBatch(splitLine,
                    batchSize,
                    dataSet,
                    config,
                    stopAfter
            );
            t.execute();
            Reporter reporter = new GoogleSheetsReporter(spreadsheet, t.sheetName());

            reporter.writeNewLine(t.sheetValues());

        } catch (ConnectionException e) {
            e.printStackTrace();
            SpeedBenchmark.end(BenchmarkLevel.BENCHMARK, "Benchmark crashed");
            SpeedBenchmark.disable();
        } catch (IOException e) {
            e.printStackTrace();
            SpeedBenchmark.end(BenchmarkLevel.BENCHMARK, "Writing to GoogleSheets crashed");
            SpeedBenchmark.disable();
        } finally {
            tearDown();
        }
    }

    public static void setUp() {
        SpeedBenchmark.enable();
        SpeedBenchmark.addEventListener(e -> {
            if (e.getLevel() == BenchmarkLevel.ALGORITHM
                    || e.getLevel() == BenchmarkLevel.BATCH)
                System.out.println(e);
        });
        SpeedBenchmark.begin(BenchmarkLevel.BENCHMARK);
    }

    public static void tearDown() {
        SpeedBenchmark.end(BenchmarkLevel.BENCHMARK, "Finished complete benchmark");
        SpeedBenchmark.disable();
    }


}
