package org.mp.naumann.processor.handler.database;

import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.Table;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.handler.BatchHandler;

public class PassThroughDatabaseBatchHandler implements BatchHandler {

    private final DataConnector dataConnector;

    public PassThroughDatabaseBatchHandler(DataConnector dataConnector) {
        this.dataConnector = dataConnector;
    }

    public void handleBatch(Batch batch) {
        Table table = dataConnector.getTable(batch.getTableName());
        table.execute(batch);
    }
}
