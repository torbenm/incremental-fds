package org.mp.naumann.testcases;

import org.mp.naumann.FileSource;
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
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.source.FixedSizeBatchSource;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.fake.FakeDatabaseBatchHandler;
import org.mp.naumann.processor.handler.BatchHandler;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import ResourceConnection.ResourceConnector;

public class InitialAndIncrementalOneBatch implements TestCase, SpeedEventListener {


    private final int splitLine;
    private final int batchSize;
    private final String filename;
    private final IncrementalFDConfiguration config;
    private int stopAfter;
    private final IncrementalFDResultListener resultListener = new IncrementalFDResultListener();
    private final List<SpeedEvent> batchEvents = new ArrayList<>();
    private final List<SpeedEvent> initialEvents = new ArrayList<>();

    private int status  = 0;

    public InitialAndIncrementalOneBatch(int splitLine, int batchSize, String filename, IncrementalFDConfiguration config, int stopAfter) {
        this.splitLine = splitLine;
        this.batchSize = batchSize;
        this.filename = filename;
        this.stopAfter = stopAfter;
        this.config = config;
        SpeedBenchmark.addEventListener(this);
    }

    @Override
    public void execute() throws ConnectionException, IOException {

        FileSource fs = new FileSource(filename, splitLine, batchSize);
        fs.doSplit();

        String tableName = "baseline";
        String schema = "benchmark";

        //Initial
        status = 1;
        //TODO: execute for EVERY batch
        try (DataConnector dc = new JdbcDataConnector(ConnectionManager.getCsvConnection(ResourceConnector.BENCHMARK, ";"))) {
            StreamableBatchSource batchSource = new FixedSizeBatchSource(FileSource.INSERTS_PATH, schema, tableName, batchSize, stopAfter);
            DatabaseBatchHandler databaseBatchHandler = new FakeDatabaseBatchHandler();
            BatchProcessor batchProcessor = new SynchronousBatchProcessor(batchSource, databaseBatchHandler);

            Table table = dc.getTable(filename.split("\\.")[0], filename.split("\\.")[1]);
            HyFDInitialAlgorithm initialAlgorithm = new HyFDInitialAlgorithm(config, table);
            batchProcessor.addBatchHandler(new InitialBatchHandler(splitLine, table, initialAlgorithm));
            SpeedBenchmark.begin(BenchmarkLevel.ALGORITHM);
            batchSource.startStreaming();
            SpeedBenchmark.end(BenchmarkLevel.ALGORITHM, "Initial algorithm for all batches");
        }

        //Incremental
        status = 2;
        FDIntermediateDatastructure ds;
        try (DataConnector dc = new JdbcDataConnector(ConnectionManager.getCsvConnectionFromAbsolutePath(FileSource.TEMP_DIR, ";"))) {
            Table table = dc.getTable(schema, tableName);
            HyFDInitialAlgorithm initialAlgorithm = new HyFDInitialAlgorithm(config, table);
            initialAlgorithm.execute();
            ds = initialAlgorithm.getIntermediateDataStructure();
        }

        StreamableBatchSource batchSource = new FixedSizeBatchSource(FileSource.INSERTS_PATH, schema, tableName, batchSize, stopAfter);
        DatabaseBatchHandler databaseBatchHandler = new FakeDatabaseBatchHandler();
        BatchProcessor batchProcessor = new SynchronousBatchProcessor(batchSource, databaseBatchHandler);

        IncrementalFD incrementalAlgorithm = new IncrementalFD(tableName, config);
        incrementalAlgorithm.initialize(ds);
        incrementalAlgorithm.addResultListener(resultListener);

        batchProcessor.addBatchHandler(incrementalAlgorithm);
        SpeedBenchmark.begin(BenchmarkLevel.ALGORITHM);
        batchSource.startStreaming();
        SpeedBenchmark.end(BenchmarkLevel.ALGORITHM, "Incremental algorithm for all batches");
        fs.cleanup();
    }

    private static class InitialBatchHandler implements BatchHandler {

        private final HyFDInitialAlgorithm initialAlgorithm;
        private final Table table;

        public InitialBatchHandler(int initialLine, Table table, HyFDInitialAlgorithm initialAlgorithm) {
            this.table = table;
            table.setLimit(initialLine);
            this.initialAlgorithm = initialAlgorithm;
        }

        @Override
        public void handleBatch(Batch batch) {
            int size = batch.getInsertStatements().size();
            table.setLimit(table.getLimit() + size);
            initialAlgorithm.execute();
        }
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
                splitLine,
                batchSize,
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

}
