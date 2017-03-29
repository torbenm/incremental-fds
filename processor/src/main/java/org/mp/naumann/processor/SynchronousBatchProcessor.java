package org.mp.naumann.processor;

import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.source.BatchSource;
import org.mp.naumann.processor.handler.BatchHandler;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * A synchronous implementation of the {@link BatchProcessor}.
 * This means the batches are processed by the {@link BatchHandler} one after another.
 */
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

    /**
     * Processes the batch in a synchronous fashion
     *
     * @param batch The batch to process
     */
    protected void distributeBatch(Batch batch) {
        for (BatchHandler batchHandler : getBatchHandlers()) {
            batchHandler.handleBatch(batch);
        }
    }
}
