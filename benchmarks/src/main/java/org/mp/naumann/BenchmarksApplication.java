package org.mp.naumann;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import org.mp.naumann.algorithms.benchmark.better.Benchmark;
import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration.PruningStrategy;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.reporter.FileReporter;
import org.mp.naumann.reporter.GoogleSheetsReporter;
import org.mp.naumann.reporter.Reporter;
import org.mp.naumann.testcases.*;

import java.io.IOException;
import java.util.logging.Level;

import ResourceConnection.ResourceConnector;

public class BenchmarksApplication {

    // general parameters
    @Parameter(names = "--name")
    private String name = "";
    @Parameter(names = "--help", help = true)
    private boolean help = false;
    @Parameter(names = "--spreadsheet")
    private String spreadsheet = "1ATQM5p6usBFImtxrn1yti-cjAtlnuli8kM4-ZpgrwfY";
    @Parameter(names = "--dataSet")
    private String dataSet = "";
    @Parameter(names = "--stopAfter")
    private int stopAfter = 1000;
    @Parameter(names = "--google")
    private boolean writeToGoogleSheets = false;
    @Parameter(names = "--sheetName")
    private String sheetName = "benchmark (new)";
    @Parameter(names = "--logLevel")
    private String logLevel = "info";

    // optional postgres parameters
    @Parameter(names = "--pgdb")
    private String pgdb = null;
    @Parameter(names = "--pguser")
    private String pguser = null;
    @Parameter(names = "--pgpass")
    private String pgpass = null;

    // parameters for the specific modes
    @Parameter(names = "--mode", description = "either variable, fixed, or singleFile")
    private String mode = "variable";
    @Parameter(names = "--batchSize", description = "only relevant for fixed or singleFile mode")
    private int batchSize = 100;
    @Parameter(names = "--splitLine", description = "only relevant for singleFile mode")
    private int splitLine = 15000;
    @Parameter(names = "--batchDirectory", description = "only relevant for variable mode")
    private String batchDirectory = "";

    // parameters for algorithm configuration
    @Parameter(names = "--hyfdOnly")
    private boolean hyfdOnly = false;
    @Parameter(names = "--hyfdCreateIndex", description = "create an index on the db table, only relevant for hyfd mode", arity = 1)
    private boolean hyfdCreateIndex = true;
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
    @Parameter(names = "--simpleBloom", arity = 1)
    private Boolean simpleBloomPruning;
    @Parameter(names = "--advancedBloom", arity = 1)
    private Boolean advancedBloomPruning;
    @Parameter(names = "--simplePruning", arity = 1)
    private Boolean simplePruning;
    @Parameter(names = "--deletePruning", arity = 1)
    private Boolean deletePruning;
    @Parameter(names = "--betterSampling", arity = 1)
    private Boolean betterSampling;

    public static void main(String[] args) throws IOException {
        BenchmarksApplication app = new BenchmarksApplication();
        JCommander jc = new JCommander(app, args);
        if (app.help) {
            jc.usage();
            System.exit(0);
        }
        app.run();
    }

    private void adjustConfiguration(IncrementalFDConfiguration config) {
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
        if (simplePruning != null) {
            config.addPruningStrategy(PruningStrategy.SIMPLE);
        }
        if (simpleBloomPruning != null) {
            config.addPruningStrategy(PruningStrategy.BLOOM);
        }
        if (advancedBloomPruning != null) {
            config.addPruningStrategy(PruningStrategy.BLOOM_ADVANCED);
        }
        if (betterSampling != null) {
            config.setImprovedSampling(betterSampling);
        }
        if (deletePruning != null) {
            config.addPruningStrategy(PruningStrategy.DELETE_ANNOTATIONS);
        }
    }

    private String getFullBatchDirectory() {
        switch (batchDirectory) {
            case "inserts":
                return ResourceConnector.INSERTS;
            case "deletes":
                return ResourceConnector.DELETES;
            case "updates":
                return ResourceConnector.UPDATES;
            case "batches":
                return ResourceConnector.BATCHES;
            default:
                return batchDirectory;
        }
    }

    private void run() throws IOException {
        setLogLevel();
        setUp();

        if (name.isEmpty())
            name = (hyfdOnly ? "hyfd" : "incremental") + ", " + mode + (mode.equals("variable") ? " (" + batchDirectory + ")" : "");

        IncrementalFDConfiguration config = IncrementalFDConfiguration.getVersion(name);
        adjustConfiguration(config);

        try {
            TestCase t;
            TestCaseParameters parameters = new TestCaseParameters("", dataSet, config, stopAfter, hyfdOnly, hyfdCreateIndex, pgdb, pguser, pgpass);

            switch (mode) {
                case "variable":
                    t = new VariableSizeTestCase(parameters, getFullBatchDirectory());
                    break;
                case "fixed":
                    t = new FixedSizeTestCase(parameters, batchSize);
                    break;
                case "singleFile":
                    t = new SingleFileTestCase(parameters, splitLine, batchSize);
                    break;
                default:
                    throw new IllegalArgumentException(String.format("Invalid mode parameter: %s", mode));
            }

            t.execute();

            Reporter reporter = (writeToGoogleSheets
                    ? new GoogleSheetsReporter(spreadsheet, sheetName)
                    : new FileReporter("report.txt")
            );
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

    private void setLogLevel() {
        try {
            FDLogger.setLevel(Level.parse(logLevel.toUpperCase()));
        } catch(IllegalArgumentException e){
            FDLogger.setLevel(Level.INFO);
        }
    }

    public static void setUp() {
        SpeedBenchmark.enable();
        SpeedBenchmark.addEventListener(e -> {
            if (e.getLevel() == BenchmarkLevel.ALGORITHM || e.getLevel() == BenchmarkLevel.BATCH)
                FDLogger.log(Level.INFO, e.toString());
        });
        SpeedBenchmark.begin(BenchmarkLevel.BENCHMARK);
        Benchmark.setMaxLevel(4);
        Benchmark.addEventListener(FDLogger::info);
    }

    public static void tearDown() {
        SpeedBenchmark.end(BenchmarkLevel.BENCHMARK, "Finished complete benchmark");
        SpeedBenchmark.disable();
    }

}