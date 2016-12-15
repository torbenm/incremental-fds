package org.mp.naumann.benchmarks;

import org.mp.naumann.algorithms.IncrementalAlgorithm;
import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.fd.HyFDInitialAlgorithm;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFD;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDVersion;
import org.mp.naumann.algorithms.fd.utils.IncrementalFDResultListener;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.jdbc.JdbcDataConnector;
import org.mp.naumann.processor.SynchronousBatchProcessor;
import org.mp.naumann.processor.batch.source.CsvFileBatchSource;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.fake.FakeDatabaseBatchHandler;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;

import java.sql.Connection;

public class IncrementalFDBenchmark implements AlgorithmBenchmark {

    private HyFDInitialAlgorithm initialAlgorithm;
    private IncrementalFD incrementalAlgorithm;
    private String currentTestCase;
    private StreamableBatchSource batchSource;
    private SynchronousBatchProcessor batchProcessor;
    private IncrementalFDResultListener resultListener;
    private final IncrementalFDVersion version;

    public IncrementalFDBenchmark(IncrementalFDVersion version) {
        this.version = version;
    }

    public void constructTestCase(String testCase, String incrementalFilePath, Connection csvConnection, String schema, String tableName, int batchSize,
                                  int stopAfter) throws ConnectionException {
        reset();
        this.currentTestCase = testCase;

        batchSource = new CsvFileBatchSource(incrementalFilePath, "", "", batchSize, stopAfter);
        DatabaseBatchHandler databaseBatchHandler = new FakeDatabaseBatchHandler();

        DataConnector dc = new JdbcDataConnector(csvConnection);
        Table table = dc.getTable(schema, tableName);
        initialAlgorithm = new HyFDInitialAlgorithm(table);

        incrementalAlgorithm = new IncrementalFD(((CsvFileBatchSource)batchSource).getColumnNames(), tableName, version);

        this.batchProcessor = new SynchronousBatchProcessor(batchSource, databaseBatchHandler);
        this.batchProcessor.addBatchHandler(incrementalAlgorithm);
    }

    public void constructInitialOnly(IncrementalFDVersion version, String testCase, Connection csvConnection, String schema, String tableName) throws ConnectionException {
        reset();
        this.currentTestCase = testCase;

        DataConnector dc = new JdbcDataConnector(csvConnection);
        Table table = dc.getTable(schema, tableName);
        initialAlgorithm = new HyFDInitialAlgorithm(table);
    }

    public void constructInitialOnly(String testCase, Connection csvConnection, String schema, String tableName) throws ConnectionException {
        constructInitialOnly(version, testCase, csvConnection, schema, tableName);
    }

    private void reset(){
        this.incrementalAlgorithm = null;
        this.initialAlgorithm = null;
        this.currentTestCase = null;
        this.batchProcessor = null;
        this.batchSource = null;
    }

    public void setIncrementalFDResultListener(IncrementalFDResultListener resultListener){
        this.resultListener = resultListener;
    }

    public String getVersionName(){
        return version.getVersionName();
    }

    public int getVersionCode(){
        return version.getShortId();
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
        incrementalAlgorithm.addResultListener(resultListener);
        SpeedBenchmark.begin(BenchmarkLevel.ALGORITHM);
        getBatchSource().startStreaming();
        SpeedBenchmark.end(BenchmarkLevel.ALGORITHM, "Finished incremental algorithm for test case "+getCurrentTestCase());

    }
}