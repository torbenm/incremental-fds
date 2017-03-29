package org.mp.naumann.benchmarks;

import org.mockito.Mockito;
import org.mp.naumann.BenchmarksApplication;
import org.mp.naumann.algorithms.IncrementalAlgorithm;
import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.implementations.AverageIncrementalAlgorithm;
import org.mp.naumann.algorithms.implementations.AverageInitialAlgorithm;
import org.mp.naumann.data.ResourceConnector;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.jdbc.JdbcDataConnector;
import org.mp.naumann.database.utils.ConnectionManager;
import org.mp.naumann.processor.SynchronousBatchProcessor;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.batch.source.csv.FixedSizeCsvBatchSource;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;

public class AverageAlgorithmBenchmark implements AlgorithmBenchmark {

    private AverageInitialAlgorithm initialAlgorithm;
    private AverageIncrementalAlgorithm incrementalAlgorithm;
    private String currentTestCase;
    private StreamableBatchSource batchSource;
    private SynchronousBatchProcessor batchProcessor;

    public void constructTestCase(String testCase, String incrementalFileName, int batchSize,
                                  String column, String table) throws ConnectionException {
        this.currentTestCase = testCase;
        String file = BenchmarksApplication.class.getResource(incrementalFileName).getPath();
        batchSource = new FixedSizeCsvBatchSource(file, "", "", batchSize);
        DatabaseBatchHandler databaseBatchHandler = Mockito.mock(DatabaseBatchHandler.class);

        JdbcDataConnector jdbcDataConnector = new JdbcDataConnector(ConnectionManager.getCsvConnection(ResourceConnector.BENCHMARK, ","));
        initialAlgorithm = new AverageInitialAlgorithm(column, table, jdbcDataConnector, "benchmark");

        incrementalAlgorithm = new AverageIncrementalAlgorithm(column);
        incrementalAlgorithm.initialize(initialAlgorithm.getIntermediateDataStructure());
        this.batchProcessor = new SynchronousBatchProcessor(batchSource, databaseBatchHandler);
        this.batchProcessor.addBatchHandler(incrementalAlgorithm);
    }


    @Override
    public String getCurrentTestCase() {
        return currentTestCase;
    }

    @Override
    public StreamableBatchSource getBatchSource() {
        return batchSource;
    }


    @Override
    public InitialAlgorithm getInitialAlgorithm() {
        return initialAlgorithm;
    }

    @Override
    public IncrementalAlgorithm getIncrementalAlgorithm() {
        return incrementalAlgorithm;
    }
}
