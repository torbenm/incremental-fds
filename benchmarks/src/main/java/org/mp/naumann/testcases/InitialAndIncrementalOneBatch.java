package org.mp.naumann.testcases;

import org.mp.naumann.FileSource;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.fd.HyFDInitialAlgorithm;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.utils.ConnectionManager;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.source.FixedSizeBatchSource;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.handler.BatchHandler;

import java.io.IOException;
import java.sql.Connection;

public class InitialAndIncrementalOneBatch extends BaseTestCase {

    private final int splitLine;
    private final int batchSize;
    private FileSource fileSource;

    public InitialAndIncrementalOneBatch(int splitLine, int batchSize, String filename, IncrementalFDConfiguration config, int stopAfter) {
        super(filename, config, stopAfter);
        this.splitLine = splitLine;
        this.batchSize = batchSize;
        SpeedBenchmark.addEventListener(this);
    }

    @Override
    void setup() {
        fileSource = new FileSource(filename, splitLine, batchSize);
        try {
            fileSource.doSplit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    void teardown() {
        fileSource.cleanup();
    }

    protected String getBatchSize() { return Integer.toString(batchSize); }

    protected StreamableBatchSource getBatchSource(String schema, String tableName, int stopAfter) {
        return new FixedSizeBatchSource(FileSource.INSERTS_PATH, schema, tableName, batchSize, stopAfter);
    }

    @Override
    protected Connection getCsvConnection() throws ConnectionException {
        return ConnectionManager.getCsvConnectionFromAbsolutePath(FileSource.TEMP_DIR, ",");
    }

    protected BatchHandler getInitialBatchHandler(Table table, HyFDInitialAlgorithm algorithm) {
        return new InitialBatchHandler(splitLine, table, algorithm);
    }

    private static class InitialBatchHandler implements BatchHandler {

        private final HyFDInitialAlgorithm initialAlgorithm;
        private final Table table;

        InitialBatchHandler(int initialLine, Table table, HyFDInitialAlgorithm initialAlgorithm) {
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
}