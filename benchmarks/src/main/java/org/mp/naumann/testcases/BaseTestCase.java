package org.mp.naumann.testcases;

import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.benchmark.speed.SpeedEvent;
import org.mp.naumann.algorithms.benchmark.speed.SpeedEventListener;
import org.mp.naumann.algorithms.fd.FDIntermediateDatastructure;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.HyFDInitialAlgorithm;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFD;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDResult;
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
import org.mp.naumann.processor.fake.FakeDatabaseBatchHandler;
import org.mp.naumann.processor.handler.BatchHandler;
import org.mp.naumann.processor.handler.database.PassThroughDatabaseBatchHandler;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;

abstract class BaseTestCase implements TestCase, SpeedEventListener {

    private final IncrementalFDConfiguration config;
    private final IncrementalFDResultListener resultListener = new IncrementalFDResultListener();
    private final List<SpeedEvent> batchEvents = new ArrayList<>();

    final int stopAfter;
    final String schema, tableName, sourceTableName;
    private final String pgdb, pgpass, pguser;
    private final boolean hyfdOnly, hyfdCreateIndex;
    private long baselineSize;

    BaseTestCase(TestCaseParameters parameters) {
        this.schema = parameters.schema;
        this.sourceTableName = parameters.tableName;
        this.tableName = parameters.tableName + (parameters.hyfdOnly ? "_tmp" : "");
        this.config = parameters.config;
        this.stopAfter = parameters.stopAfter;
        this.hyfdOnly = parameters.hyfdOnly;
        this.hyfdCreateIndex = parameters.hyfdCreateIndex;
        this.pgdb = parameters.pgdb;
        this.pguser = parameters.pguser;
        this.pgpass = parameters.pgpass;
        SpeedBenchmark.addEventListener(this);
    }

    @Override
    public void execute() throws ConnectionException, IOException {
        try (Connection conn = ConnectionManager.getPostgresConnection(pgdb, pguser, pgpass); DataConnector dc = new JdbcDataConnector(conn)) {

            Table table = dc.getTable(schema, sourceTableName);
            setBaselineSize(table.getRowCount());
            StreamableBatchSource batchSource = getBatchSource();

            // execute HyFD in any case; we need the data structure for the incremental algorithm, and can use it
            // as warmup if we run in hyfdOnly mode
            HyFDInitialAlgorithm initialAlgorithm = new HyFDInitialAlgorithm(config, table);
            initialAlgorithm.execute();
            List<FunctionalDependency> fds = initialAlgorithm.getFDs();
            FDLogger.log(Level.INFO, String.format("Initial FD count: %s", fds.size()));
            FDLogger.log(Level.FINE, "Initial FDs:");
            fds.forEach(fd -> FDLogger.log(Level.FINE, fd.toString()));

            if (hyfdOnly) {
                // create temporary table that we can modify as batches come in
                String fullTableName = (schema.isEmpty() ? "" : schema + ".") + tableName;
                Statement stmt = conn.createStatement();
                stmt.execute("CREATE TEMPORARY TABLE " + fullTableName + " AS SELECT * FROM " + sourceTableName);

                dc.clearTableNames(schema);
                table = dc.getTable(schema, tableName);

                if (hyfdCreateIndex) {
                    // create index on the temporary table
                    stmt.execute(String.format("CREATE INDEX %s_master_idx ON %s (%s)", tableName, fullTableName, String.join(", ", table.getColumnNames())));
                }

                BatchProcessor batchProcessor = new SynchronousBatchProcessor(batchSource, new PassThroughDatabaseBatchHandler(dc), true);
                batchProcessor.addBatchHandler(new HyFDBatchHandler(table, getLimit(), config, resultListener));
            } else {
                FDIntermediateDatastructure ds = initialAlgorithm.getIntermediateDataStructure();
                IncrementalFD incrementalAlgorithm = new IncrementalFD(sourceTableName, config);

                incrementalAlgorithm.initialize(ds);
                incrementalAlgorithm.addResultListener(resultListener);
                BatchProcessor batchProcessor = new SynchronousBatchProcessor(batchSource, new FakeDatabaseBatchHandler(), false);
                batchProcessor.addBatchHandler(incrementalAlgorithm);
            }

            SpeedBenchmark.begin(BenchmarkLevel.ALGORITHM);
            batchSource.startStreaming();
            SpeedBenchmark.end(BenchmarkLevel.ALGORITHM, "Algorithm for all batches");

            System.out.println(String.format("Cumulative runtime (algorithm only): %sms", getTotalTime(batchEvents)));
            System.out.println(String.format("Found %s FDs:", resultListener.getFDs().size()));
            resultListener.getFDs().forEach(fd -> FDLogger.log(Level.INFO, fd.toString()));

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
                getTotalTime(batchEvents),
                resultListener.getValidationCount(),
                resultListener.getPrunedCount(),
                resultListener.getFDs().size()
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

    private long getTotalTime(List<SpeedEvent> events){
        return events
                .stream()
                .mapToLong(SpeedEvent::getDuration)
                .sum();
    }

    int getLimit() { return 0; }

    abstract protected String getBatchSize();

    abstract protected StreamableBatchSource getBatchSource();

    protected void setBaselineSize(long baselineSize) {
        this.baselineSize = baselineSize;
    }

    private static class HyFDBatchHandler implements BatchHandler {

        private final Table table;
        private final boolean singleFile;
        private final IncrementalFDConfiguration config;
        private final IncrementalFDResultListener resultListener;

        HyFDBatchHandler(Table table, int limit, IncrementalFDConfiguration config, IncrementalFDResultListener resultListener) {
            this.table = table;
            this.config = config;
            this.resultListener = resultListener;
            singleFile = (limit > 0);
            if (singleFile) table.setLimit(limit);
        }

        @Override
        public void handleBatch(Batch batch) {
            if (singleFile) {
                int size = batch.getInsertStatements().size();
                table.setLimit(table.getLimit() + size);
            }

            HyFDInitialAlgorithm algorithm = new HyFDInitialAlgorithm(config, table);
            algorithm.execute();

            IncrementalFDResult result = new IncrementalFDResult(algorithm.getFDs(), algorithm.getValidationCount(), 0);
            resultListener.receiveResult(result);
        }
    }

}