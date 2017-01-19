package org.mp.naumann.benchmarks;

import ResourceConnection.ResourceType;
import org.mp.naumann.BenchmarksApplication;
import org.mp.naumann.algorithms.IncrementalAlgorithm;
import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.implementations.AverageIncrementalAlgorithm;
import org.mp.naumann.algorithms.implementations.AverageInitialAlgorithm;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.jdbc.JdbcDataConnector;
import org.mp.naumann.database.utils.ConnectionManager;
import org.mp.naumann.processor.SynchronousBatchProcessor;
import org.mp.naumann.processor.batch.source.CsvFileBatchSource;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.fake.FakeDatabaseBatchHandler;
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
        batchSource = new CsvFileBatchSource(file, "", "", batchSize);
        DatabaseBatchHandler databaseBatchHandler = new FakeDatabaseBatchHandler();

        JdbcDataConnector jdbcDataConnector = new JdbcDataConnector(ConnectionManager.getCsvConnection(ResourceType.BENCHMARK, ","));
        initialAlgorithm = new AverageInitialAlgorithm(column, table, jdbcDataConnector, "benchmark");

        incrementalAlgorithm = new AverageIncrementalAlgorithm(column);
        incrementalAlgorithm.setIntermediateDataStructure(initialAlgorithm.getIntermediateDataStructure());
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
