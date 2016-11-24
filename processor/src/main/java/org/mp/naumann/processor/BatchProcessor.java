package org.mp.naumann.processor;

import java.util.Collection;

import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.source.BatchSource;
import org.mp.naumann.processor.batch.source.BatchSourceListener;
import org.mp.naumann.processor.handler.BatchHandler;
import org.mp.naumann.processor.handler.DataAwareBatchHandler;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;

public abstract class BatchProcessor implements BatchSourceListener {

    private final Collection<BatchHandler> batchHandlerCollection;
    private final DatabaseBatchHandler databaseBatchHandler;

    public BatchProcessor(BatchSource batchSource, DatabaseBatchHandler databaseBatchHandler){
        this.databaseBatchHandler = databaseBatchHandler;
        this.batchHandlerCollection = initializeBatchHandlerCollection();
        batchSource.addBatchSourceListener(this);
    }

    protected abstract Collection<BatchHandler> initializeBatchHandlerCollection();

    public void addBatchHandler(BatchHandler batchHandler){
        batchHandlerCollection.add(batchHandler);
    }

    public void addDataAwareBatchHandler(DataAwareBatchHandler batchHandler){
    	batchHandler.setDataConnector(databaseBatchHandler.getConnector());
        batchHandlerCollection.add(batchHandler);
    }

    public void removeBatchHandler(BatchHandler batchHandler){
        batchHandlerCollection.remove(batchHandler);
    }

    protected Collection<BatchHandler> getBatchHandlers(){
        return batchHandlerCollection;
    }

    public void batchArrived(Batch batch){
        distributeBatch(batch);
        databaseBatchHandler.handleBatch(batch);
    }

    protected abstract void distributeBatch(Batch batch);

    public DatabaseBatchHandler getDatabaseBatchHandler() {
        return databaseBatchHandler;
    }
}
