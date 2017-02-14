package org.mp.naumann.testcases;

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
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.handler.BatchHandler;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;
import org.mp.naumann.processor.handler.database.PassThroughDatabaseBatchHandler;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

abstract class BaseTestCase implements TestCase, SpeedEventListener {

    private final IncrementalFDConfiguration config;
    private final IncrementalFDResultListener resultListener = new IncrementalFDResultListener();
    private final List<SpeedEvent> batchEvents = new ArrayList<>();

    final int stopAfter;
    final String schema, tableName, sourceTableName;
    private final boolean hyfdOnly;
    private int fdCount;
    private long baselineSize;

    BaseTestCase(String schema, String tableName, IncrementalFDConfiguration config, int stopAfter, boolean hyfdOnly) {
        this.schema = schema;
        this.sourceTableName = tableName;
        this.tableName = tableName + "_tmp";
        this.config = config;
        this.stopAfter = stopAfter;
        this.hyfdOnly = hyfdOnly;
        SpeedBenchmark.addEventListener(this);
    }

    @Override
    public void execute() throws ConnectionException, IOException {
        try (Connection conn = ConnectionManager.getPostgresConnection(); DataConnector dc = new JdbcDataConnector(conn)) {

            // create temporary table that we can modify as batches come in
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TEMPORARY TABLE " + (schema.isEmpty() ? "" : schema + ".") + tableName + " AS SELECT * FROM " + sourceTableName);

            DatabaseBatchHandler databaseBatchHandler = new PassThroughDatabaseBatchHandler(dc);
            StreamableBatchSource batchSource = getBatchSource();
            BatchProcessor batchProcessor = new SynchronousBatchProcessor(batchSource, databaseBatchHandler, hyfdOnly);
            Table table = dc.getTable(schema, tableName);
            baselineSize = table.getRowCount();
            HyFDInitialAlgorithm initialAlgorithm = new HyFDInitialAlgorithm(config, table);

            if (hyfdOnly) {
                batchProcessor.addBatchHandler(new HyFDBatchHandler(table, initialAlgorithm, getLimit()));
            } else {
                FDIntermediateDatastructure ds;
                initialAlgorithm.execute();
                ds = initialAlgorithm.getIntermediateDataStructure();

                IncrementalFD incrementalAlgorithm = new IncrementalFD(sourceTableName, config);
                incrementalAlgorithm.initialize(ds);
                incrementalAlgorithm.addResultListener(resultListener);

                batchProcessor.addBatchHandler(incrementalAlgorithm);
            }

            SpeedBenchmark.begin(BenchmarkLevel.ALGORITHM);
            batchSource.startStreaming();
            SpeedBenchmark.end(BenchmarkLevel.ALGORITHM, "Algorithm for all batches");

            fdCount = (hyfdOnly ? initialAlgorithm.getFDs().size() : resultListener.getFDs().size());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Object[] sheetValues() {
        return new Object[]{
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()),
                sourceTableName,
                config.getVersionName(),
                baselineSize,
                getBatchSize(),
                batchEvents.size(),
                getAverageTime(batchEvents),
                getMedianTime(batchEvents),
                (hyfdOnly ? 0 : resultListener.getValidationCount()),
                (hyfdOnly ? 0 : resultListener.getPrunedCount()),
                fdCount
        };
    }

    @Override
    public void receiveEvent(SpeedEvent info) {
        if (info.getLevel() == BenchmarkLevel.BATCH) batchEvents.add(info);
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

    int getLimit() { return 0; }

    abstract protected String getBatchSize();

    abstract protected StreamableBatchSource getBatchSource();

    private static class HyFDBatchHandler implements BatchHandler {

        private final HyFDInitialAlgorithm initialAlgorithm;
        private final Table table;
        private final boolean singleFile;

        HyFDBatchHandler(Table table, HyFDInitialAlgorithm initialAlgorithm, int limit) {
            this.table = table;
            this.initialAlgorithm = initialAlgorithm;
            singleFile = (limit > 0);
            if (singleFile) table.setLimit(limit);
        }

        @Override
        public void handleBatch(Batch batch) {
            if (singleFile) {
                int size = batch.getInsertStatements().size();
                table.setLimit(table.getLimit() + size);
            }
            initialAlgorithm.execute();
        }
    }

}