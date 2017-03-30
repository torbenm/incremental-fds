package org.mp.naumann.processor;

import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.source.BatchSource;
import org.mp.naumann.processor.batch.source.BatchSourceListener;
import org.mp.naumann.processor.handler.BatchHandler;
import org.mp.naumann.processor.handler.DataAwareBatchHandler;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;

import java.util.Collection;


/**
 * BatchProcessors and its implementations take batches coming from a {@link BatchSource}
 * and distribute these to {@link BatchHandler}.
 * BatchProcessors are also aware of special BatchHandlers, which are {@link DatabaseBatchHandler}.
 * These BatchHandlers are called either first or last and take care of transmitting the
 * changes to the database in the background.
 */
public abstract class BatchProcessor implements BatchSourceListener {

    private final Collection<BatchHandler> batchHandlerCollection;
    private final DatabaseBatchHandler databaseBatchHandler;

    /**
     * Denotes whether or not the data should be forwarded to the datbase first or last.
     */
    private boolean insertToDatabaseFirst;

    BatchProcessor(BatchSource batchSource, DatabaseBatchHandler databaseBatchHandler) {
        this(batchSource, databaseBatchHandler, false);
    }

    BatchProcessor(BatchSource batchSource, DatabaseBatchHandler databaseBatchHandler, boolean insertToDatabaseFirst) {
        this.databaseBatchHandler = databaseBatchHandler;
        this.batchHandlerCollection = initializeBatchHandlerCollection();
        this.insertToDatabaseFirst = insertToDatabaseFirst;
        batchSource.addBatchSourceListener(this);
    }

    protected abstract Collection<BatchHandler> initializeBatchHandlerCollection();

    /**
     * Adds a BatchHandler which is supposed to process data coming from a {@link BatchSource}
     *
     * @param batchHandler The BatchHandler to add.
     */
    public void addBatchHandler(BatchHandler batchHandler) {
        batchHandlerCollection.add(batchHandler);
    }

    /**
     * Adds a DataAwareBatchHandler which is supposed to process data coming from a {@link
     * BatchSource}
     *
     * @param batchHandler The DataAwareBatchHandler to add.
     */
    public void addDataAwareBatchHandler(DataAwareBatchHandler batchHandler) {
        batchHandler.setDataConnector(databaseBatchHandler.getConnector());
        batchHandlerCollection.add(batchHandler);
    }

    /**
     * Removes a BatchHandler from the list of BatchHandlers to process data coming from a {@link
     * BatchSource}
     *
     * @param batchHandler The BatchHandler to remove.
     */
    public void removeBatchHandler(BatchHandler batchHandler) {
        batchHandlerCollection.remove(batchHandler);
    }

    /**
     * Returns all currently registered BatchHandlers.
     *
     * @return A Collection containing all registered BatchHandlers.
     */
    protected Collection<BatchHandler> getBatchHandlers() {
        return batchHandlerCollection;
    }

    /**
     * Takes a Batch and distributes it to the registered {@link BatchHandler} BatchHandlers. Also,
     * this method makes sure that the {@link DatabaseBatchHandler} is called either first or last.
     *
     * @param batch The Batch to process.
     */
    public void batchArrived(Batch batch) {
        if (insertToDatabaseFirst) databaseBatchHandler.handleBatch(batch);
        distributeBatch(batch);
        if (!insertToDatabaseFirst) databaseBatchHandler.handleBatch(batch);
    }

    protected abstract void distributeBatch(Batch batch);

    public DatabaseBatchHandler getDatabaseBatchHandler() {
        return databaseBatchHandler;
    }

    public boolean isInsertToDatabaseFirst() {
        return insertToDatabaseFirst;
    }

    /**
     * Denotes whether or not the data should be forwarded to the datbase first or last.
     *
     * @param insertToDatabaseFirst true = call DataBaseBatchHandler first, false = call
     *                              DatabaseBatchHandler last
     */
    void setInsertToDatabaseFirst(boolean insertToDatabaseFirst) {
        this.insertToDatabaseFirst = insertToDatabaseFirst;
    }
}
