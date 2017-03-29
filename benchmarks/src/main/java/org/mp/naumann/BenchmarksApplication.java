package org.mp.naumann;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration.PruningStrategy;
import org.mp.naumann.data.ResourceConnector;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.reporter.FileReporter;
import org.mp.naumann.reporter.GoogleSheetsReporter;
import org.mp.naumann.reporter.Reporter;
import org.mp.naumann.testcases.FixedSizeTestCase;
import org.mp.naumann.testcases.SingleFileTestCase;
import org.mp.naumann.testcases.TestCase;
import org.mp.naumann.testcases.TestCaseParameters;
import org.mp.naumann.testcases.VariableSizeTestCase;

import java.io.IOException;
import java.util.logging.Level;

import static java.lang.Double.NaN;

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
    private int stopAfter = Integer.MAX_VALUE;
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
    @Parameter(names = "--batchSizeRatio", description = "only relevant for fixed or singleFile mode")
    private double batchSizeRatio = NaN;

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
    @Parameter(names = "--simpleBloom")
    private Boolean simpleBloomPruning;
    @Parameter(names = "--advancedBloom")
    private Boolean advancedBloomPruning;
    @Parameter(names = "--simplePruning")
    private Boolean simplePruning;
    @Parameter(names = "--deletePruning")
    private Boolean deletePruning;
    @Parameter(names = "--betterSampling", arity = 1)
    private Boolean betterSampling;
    @Parameter(names = "--depthFirst", arity = 1)
    private Boolean depthFirst;

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
        if (depthFirst != null) {
            config.setDepthFirst(depthFirst);
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
                    if (batchSizeRatio == NaN)
                        t = new FixedSizeTestCase(parameters, batchSize);
                    else
                        t = new FixedSizeTestCase(parameters, batchSizeRatio);
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

        } catch (IOException e) {
            e.printStackTrace();

        }
    }

    private void setLogLevel() {
        try {
            FDLogger.setLevel(Level.parse(logLevel.toUpperCase()));
        } catch (IllegalArgumentException e) {
            FDLogger.setLevel(Level.INFO);
        }
    }
}