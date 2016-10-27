package org.mp.naumann.processor;

import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.source.BatchSource;
import org.mp.naumann.processor.batch.source.BatchSourceListener;
import org.mp.naumann.processor.handler.BatchHandler;

import java.util.Collection;

public abstract class BatchProcessor<BatchCollectionType extends Collection<BatchHandler>> implements BatchSourceListener {

    private final BatchCollectionType batchHandlerCollection;

    public BatchProcessor(BatchSource batchSource){
        batchHandlerCollection = initializeBatchHandlerCollection();
        batchSource.addBatchSourceListener(this);
    }

    protected abstract BatchCollectionType initializeBatchHandlerCollection();

    public void addBatchHandler(BatchHandler batchHandler){
        batchHandlerCollection.add(batchHandler);
    }

    public void removeBatchHandler(BatchHandler batchHandler){
        batchHandlerCollection.remove(batchHandler);
    }

    protected BatchCollectionType getBatchHandlers(){
        return batchHandlerCollection;
    }

    public void batchArrived(Batch batch){
        distributeBatch(batch);
    }

    protected abstract void distributeBatch(Batch batch);

}
