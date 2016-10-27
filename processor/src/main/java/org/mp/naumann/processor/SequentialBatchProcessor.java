package org.mp.naumann.processor;

import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.source.BatchSource;

import java.util.ArrayList;
import java.util.List;

public class SequentialBatchProcessor extends BatchProcessor<List<BatchHandler>> {

    public SequentialBatchProcessor(BatchSource batchSource) {
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
