package org.mp.naumann.algorithms.fd;

import org.mp.naumann.algorithms.benchmark.better.Benchmark;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration.PruningStrategy;
import org.mp.naumann.algorithms.fd.utils.IncrementalFDResultListener;
import org.mp.naumann.database.ConnectionException;

import java.util.List;
import java.util.logging.Level;

import ResourceConnection.ResourceConnector;

public class IncrementalFDDemo {

    private static final IncrementalFDRunConfiguration sample = new IncrementalFDRunConfiguration(
            "deletes.deletesample.csv",
            "",
            "test.deletesample",
            1800,
            ResourceConnector.TEST,
            ","
    );

    private static final IncrementalFDRunConfiguration philippSample = new IncrementalFDRunConfiguration(
            "simple.csv",
            "",
            "simple",
            1800,
            ResourceConnector.BASELINE,
            ","
    );

    private static final IncrementalFDRunConfiguration adultInsert = new IncrementalFDRunConfiguration(
            "inserts.adult.csv",
            "",
            "benchmark.adult",
            5000,
            ResourceConnector.BENCHMARK,
            ","
    );

    private static final IncrementalFDRunConfiguration adult = new IncrementalFDRunConfiguration(
            "deletes.adult.csv",
            "",
            "benchmark.adult",
            1800,
            ResourceConnector.BENCHMARK,
            ","
    );

    private static final IncrementalFDRunConfiguration bridges = new IncrementalFDRunConfiguration(
            "deletes.bridges.csv",
            "",
            "test.bridges",
            200,
            ResourceConnector.TEST,
            ","
    );

    public static void main(String[] args) throws ClassNotFoundException, ConnectionException, AlgorithmExecutionException {
        FDLogger.setLevel(Level.FINE);

        IncrementalFDConfiguration configuration = new IncrementalFDConfiguration("custom")
                .addPruningStrategy(PruningStrategy.DELETES)
//                .addPruningStrategy(PruningStrategy.BLOOM_ADVANCED)
        ;
        IncrementalFDRunConfiguration runConfig = philippSample;


        SpeedBenchmark.enable();
        Benchmark.enableAll();
        Benchmark.addEventListener(FDLogger::info);
//        SpeedBenchmark.addEventListener(System.out::println);

        IncrementalFDRunner runner = new IncrementalFDRunner() {
            @Override
            public void afterInitial(List<FunctionalDependency> dependencyList) {
                FDLogger.log(Level.INFO, String.format("Original FD count: %s", dependencyList.size()));
                FDLogger.log(Level.INFO, String.format("Batch size: %s", runConfig.getBatchSize()));
                FDLogger.log(Level.FINEST, "\n");
                dependencyList.forEach(fd -> FDLogger.log(Level.FINEST, fd.toString()));
            }

            @Override
            public void afterIncremental(IncrementalFDResultListener listener) {

                // output results
                FDLogger.log(Level.INFO, String.format("Total performed validations: %s", listener.getValidationCount()));
                FDLogger.log(Level.INFO, String.format("Total pruned validations: %s", listener.getPrunedCount()));
                FDLogger.log(Level.INFO, String.format("Final FD count: %s", listener.getFDs().size()));
                FDLogger.log(Level.FINEST, "\n");
                listener.getFDs().forEach(f -> FDLogger.log(Level.FINEST, f.toString()));
            }
        };
        runner.run(runConfig, configuration);
    }

}
