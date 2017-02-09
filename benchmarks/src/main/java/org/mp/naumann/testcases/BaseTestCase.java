package org.mp.naumann.testcases;

import ResourceConnection.ResourceConnector;
import org.apache.commons.io.FilenameUtils;
import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.benchmark.speed.SpeedEvent;
import org.mp.naumann.algorithms.benchmark.speed.SpeedEventListener;
import org.mp.naumann.algorithms.fd.FDIntermediateDatastructure;
import org.mp.naumann.algorithms.fd.HyFDInitialAlgorithm;
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
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.fake.FakeDatabaseBatchHandler;
import org.mp.naumann.processor.handler.BatchHandler;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;

import java.io.IOException;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

abstract class BaseTestCase implements TestCase, SpeedEventListener {

    final String filename;
    private final IncrementalFDConfiguration config;
    private int stopAfter;
    private final IncrementalFDResultListener resultListener = new IncrementalFDResultListener();
    private final List<SpeedEvent> batchEvents = new ArrayList<>();
    private final List<SpeedEvent> initialEvents = new ArrayList<>();

    private int status = 0;

    BaseTestCase(String filename, IncrementalFDConfiguration config, int stopAfter) {
        this.filename = filename;
        this.stopAfter = stopAfter;
        this.config = config;
        SpeedBenchmark.addEventListener(this);
    }

    @Override
    public void execute() throws ConnectionException, IOException {
        setup();

        String tableName = "wcrosters";
        String schema = "";

        //Initial
        /*status = 1;
        try (DataConnector dc = new JdbcDataConnector(ConnectionManager.getCsvConnection(ResourceConnector.BASELINE, ","))) {
            StreamableBatchSource batchSource = getBatchSource(schema, tableName, stopAfter);
            DatabaseBatchHandler databaseBatchHandler = new FakeDatabaseBatchHandler();
            BatchProcessor batchProcessor = new SynchronousBatchProcessor(batchSource, databaseBatchHandler);

            Table table = dc.getTable("", FilenameUtils.removeExtension(filename));
            HyFDInitialAlgorithm initialAlgorithm = new HyFDInitialAlgorithm(config, table);
            batchProcessor.addBatchHandler(getInitialBatchHandler(table, initialAlgorithm));
            SpeedBenchmark.begin(BenchmarkLevel.ALGORITHM);
            batchSource.startStreaming();
            SpeedBenchmark.end(BenchmarkLevel.ALGORITHM, "Initial algorithm for all batches");
        }*/

        //Incremental
        status = 2;
        FDIntermediateDatastructure ds;
        try (DataConnector dc = new JdbcDataConnector(getCsvConnection())) {
            Table table = dc.getTable(schema, tableName);
            HyFDInitialAlgorithm initialAlgorithm = new HyFDInitialAlgorithm(config, table);
            initialAlgorithm.execute();
            ds = initialAlgorithm.getIntermediateDataStructure();
        }

        StreamableBatchSource batchSource = getBatchSource(schema, tableName, stopAfter);
        DatabaseBatchHandler databaseBatchHandler = new FakeDatabaseBatchHandler();
        BatchProcessor batchProcessor = new SynchronousBatchProcessor(batchSource, databaseBatchHandler);

        IncrementalFD incrementalAlgorithm = new IncrementalFD(tableName, config);
        incrementalAlgorithm.initialize(ds);
        incrementalAlgorithm.addResultListener(resultListener);

        batchProcessor.addBatchHandler(incrementalAlgorithm);
        SpeedBenchmark.begin(BenchmarkLevel.ALGORITHM);
        batchSource.startStreaming();
        SpeedBenchmark.end(BenchmarkLevel.ALGORITHM, "Incremental algorithm for all batches");

        teardown();
    }

    @Override
    public String sheetName() {
        return "Initial over all + Incremental One Batch";
    }

    @Override
    public Object[] sheetValues() {
        return new Object[]{
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()),
                filename,
                config.getVersionName(),
                getBatchSize(),
                batchEvents.size(),
                getAverageTime(initialEvents),
                getMedianTime(initialEvents),
                getAverageTime(batchEvents),
                getMedianTime(batchEvents),
                resultListener.getValidationCount(),
                resultListener.getPrunedCount(),
                resultListener.getFDs().size()
        };
    }

    @Override
    public void receiveEvent(SpeedEvent info) {
        if(status == 1 && info.getLevel() == BenchmarkLevel.BATCH)
            initialEvents.add(info);
        else if(status == 2 && info.getLevel() == BenchmarkLevel.BATCH)
            batchEvents.add(info);
    }

    private long getAverageTime(List<SpeedEvent> events){
        return (long)events
                .stream()
                .mapToLong(SpeedEvent::getDuration)
                .average()
                .orElse(-1);
    }

    private long getMedianTime(List<SpeedEvent> events){
        return events
                .stream()
                .mapToLong(SpeedEvent::getDuration)
                .sorted()
                .skip(batchEvents.size() / 2)
                .findFirst()
                .orElse(-1);
    }

    void setup() { }

    void teardown() { }

    abstract protected String getBatchSize();

    abstract protected StreamableBatchSource getBatchSource(String schema, String tableName, int stopAfter);

    abstract protected Connection getCsvConnection() throws ConnectionException;

    abstract protected BatchHandler getInitialBatchHandler(Table table, HyFDInitialAlgorithm algorithm);

}