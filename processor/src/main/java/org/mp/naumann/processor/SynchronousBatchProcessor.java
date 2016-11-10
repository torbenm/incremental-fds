package org.mp.naumann.processor;

import java.util.ArrayList;
import java.util.List;

import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.source.BatchSource;
import org.mp.naumann.processor.handler.BatchHandler;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;

public class SynchronousBatchProcessor extends BatchProcessor<List<BatchHandler>> {

    public SynchronousBatchProcessor(BatchSource batchSource, DatabaseBatchHandler databaseBatchHandler) {
        super(batchSource, databaseBatchHandler);
    }

    protected List<BatchHandler> initializeBatchHandlerCollection() {
        return new ArrayList<>();
    }

    protected void distributeBatch(Batch batch) {
        for(BatchHandler batchHandler : getBatchHandlers()){
            batchHandler.handleBatch(batch);
        }
    }

}
