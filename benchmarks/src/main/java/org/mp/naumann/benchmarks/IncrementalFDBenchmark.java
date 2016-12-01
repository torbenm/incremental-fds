package org.mp.naumann.benchmarks;

import org.mp.naumann.BenchmarksApplication;
import org.mp.naumann.algorithms.IncrementalAlgorithm;
import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.fd.HyFDInitialAlgorithm;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFD;
import org.mp.naumann.algorithms.implementations.AverageIncrementalAlgorithm;
import org.mp.naumann.algorithms.implementations.AverageInitialAlgorithm;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.jdbc.JdbcDataConnector;
import org.mp.naumann.database.utils.ConnectionManager;
import org.mp.naumann.processor.SynchronousBatchProcessor;
import org.mp.naumann.processor.batch.source.CsvFileBatchSource;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.fake.FakeDatabaseBatchHandler;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;

import java.util.Arrays;

public class IncrementalFDBenchmark implements AlgorithmBenchmark {

    private HyFDInitialAlgorithm initialAlgorithm;
    private IncrementalFD incrementalAlgorithm;
    private String currentTestCase;
    private StreamableBatchSource batchSource;
    private SynchronousBatchProcessor batchProcessor;

    public void constructTestCase(String testCase, String incrementalFileName, String schema, String tableName, int batchSize) throws ConnectionException {
        this.currentTestCase = testCase;

        String file = BenchmarksApplication.class.getResource(incrementalFileName).getPath();
        batchSource = new CsvFileBatchSource(file, "", "", batchSize);
        DatabaseBatchHandler databaseBatchHandler = new FakeDatabaseBatchHandler();

        DataConnector dc = new JdbcDataConnector(ConnectionManager.getCsvConnection(BenchmarksApplication.class, "", ","));
        Table table = dc.getTable(schema, tableName);
        initialAlgorithm = new HyFDInitialAlgorithm(table);

        incrementalAlgorithm = new IncrementalFD(((CsvFileBatchSource)batchSource).getColumnNames(), tableName);

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

    @Override
    public void runIncremental() {
        incrementalAlgorithm.setIntermediateDataStructure(initialAlgorithm.getIntermediateDataStructure());
        SpeedBenchmark.begin(BenchmarkLevel.ALGORITHM);
        getBatchSource().startStreaming();
        SpeedBenchmark.end(BenchmarkLevel.ALGORITHM, "Finished incremental algorithm for test case "+getCurrentTestCase());

    }
}