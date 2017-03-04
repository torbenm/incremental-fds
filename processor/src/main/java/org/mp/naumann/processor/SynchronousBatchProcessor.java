package org.mp.naumann.processor;

import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.source.BatchSource;
import org.mp.naumann.processor.handler.BatchHandler;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;

import java.util.ArrayList;
import java.util.List;

public class SynchronousBatchProcessor extends BatchProcessor {

    private int batchCounter = 1;

    public SynchronousBatchProcessor(BatchSource batchSource, DatabaseBatchHandler databaseBatchHandler) {
        super(batchSource, databaseBatchHandler);
    }

    public SynchronousBatchProcessor(BatchSource batchSource, DatabaseBatchHandler databaseBatchHandler, boolean insertToDatabaseFirst) {
        super(batchSource, databaseBatchHandler, insertToDatabaseFirst);
    }

    protected List<BatchHandler> initializeBatchHandlerCollection() {
        return new ArrayList<>();
    }

    protected void distributeBatch(Batch batch) {
        SpeedBenchmark.begin(BenchmarkLevel.BATCH);
        for(BatchHandler batchHandler : getBatchHandlers()) batchHandler.handleBatch(batch);
        SpeedBenchmark.end(BenchmarkLevel.BATCH, "Processed Batch " + batchCounter++);
    }

}
