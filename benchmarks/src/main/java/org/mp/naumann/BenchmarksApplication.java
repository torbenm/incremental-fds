package org.mp.naumann;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.benchmarks.IncrementalFDBenchmark;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.reporter.GoogleSheetsReporter;
import org.mp.naumann.testcases.InitialAndIncrementalOneBatch;
import org.mp.naumann.testcases.TestCase;

import java.io.IOException;
import java.util.logging.Level;

public class BenchmarksApplication {

    @Parameter(names = "--name")
    private String name = "";
    @Parameter(names = "--version")
    private int version = -1;
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
    @Parameter(names = "--sampling")
    private Boolean useSampling;
    @Parameter(names = "--clusterPruning")
    private Boolean useClusterPruning;
    @Parameter(names = "--innerClusterPruning")
    private Boolean useInnerClusterPruning;


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
        GoogleSheetsReporter googleSheetsReporter = new GoogleSheetsReporter(spreadsheet);
        int stopAfter = batchSize < 10 ? 100 : -1;

        FDLogger.setLevel(Level.OFF);
        setUp();

        IncrementalFDConfiguration config = IncrementalFDConfiguration.getVersion(version, name);
        if (useSampling != null) {
            config.setSampling(useSampling);
        }
        if (useClusterPruning != null) {
            config.setClusterPruning(useClusterPruning);
        }
        if (useInnerClusterPruning != null) {
            config.setInnerClusterPruning(useInnerClusterPruning);
        }

        try {
            TestCase t = new InitialAndIncrementalOneBatch(splitLine,
                    batchSize,
                    dataSet,
                    new IncrementalFDBenchmark(config),
                    stopAfter
            );
            t.execute();

            googleSheetsReporter.writeNewLine(
                    t.sheetName(),
                    t.sheetValues()
            );

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
