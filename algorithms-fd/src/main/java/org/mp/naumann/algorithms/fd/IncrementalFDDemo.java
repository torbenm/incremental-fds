package org.mp.naumann.algorithms.fd;

import ResourceConnection.ResourceConnector;
import ResourceConnection.ResourceType;

import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFD;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.utils.IncrementalFDResultListener;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.jdbc.JdbcDataConnector;
import org.mp.naumann.database.utils.ConnectionManager;
import org.mp.naumann.processor.BatchProcessor;
import org.mp.naumann.processor.SynchronousBatchProcessor;
import org.mp.naumann.processor.batch.source.FixedSizeBatchSource;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.fake.FakeDatabaseBatchHandler;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;

import java.util.List;
import java.util.logging.Level;

public class IncrementalFDDemo {

    private static final IncrementalFDRunConfiguration sample = new IncrementalFDRunConfiguration(
            "deletes.deletesample.csv",
            "",
            "test.deletesample",
            1800,
                ResourceType.TEST,
            ","
    );

    private static final IncrementalFDRunConfiguration adult = new IncrementalFDRunConfiguration(
            "deletes.adult.csv",
            "",
            "benchmark.adult",
            1800,
            ResourceType.BENCHMARK,
            ","
    );

    private static final IncrementalFDRunConfiguration bridges = new IncrementalFDRunConfiguration(
            "deletes.bridges.csv",
            "",
            "test.bridges",
            200,
            ResourceType.TEST,
            ","
    );

    public static void main(String[] args) throws ClassNotFoundException, ConnectionException, AlgorithmExecutionException {
        FDLogger.setLevel(Level.INFO);

        IncrementalFDConfiguration configuration = new IncrementalFDConfiguration("custom")
                .addPruningStrategy(IncrementalFDConfiguration.PruningStrategy.ANNOTATION);

        IncrementalFDRunConfiguration runConfig = bridges;


        SpeedBenchmark.enable();
        SpeedBenchmark.addEventListener(f -> {
                    if(f.getLevel() == BenchmarkLevel.UNIQUE) System.out.println(f);
        }
        );

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
                //listener.getFDs().forEach(f -> FDLogger.log(Level.INFO, f.toString()));
            }
        };
        runner.run(runConfig, configuration);
    }

}
