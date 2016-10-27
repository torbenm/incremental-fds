package org.mp.naumann.processor;

import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.source.BatchSource;
import org.mp.naumann.processor.handler.BatchHandler;

import java.util.ArrayList;
import java.util.List;

public class SynchronousBatchProcessor extends BatchProcessor<List<BatchHandler>> {

    public SynchronousBatchProcessor(BatchSource batchSource) {
        super(batchSource);
    }

    protected List<BatchHandler> initializeBatchHandlerCollection() {
        return new ArrayList<BatchHandler>();
    }

    protected void distributeBatch(Batch batch) {
        for(BatchHandler batchHandler : getBatchHandlers()){
            batchHandler.handleBatch(batch);
        }
    }
}