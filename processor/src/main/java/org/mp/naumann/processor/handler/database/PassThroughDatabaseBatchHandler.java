package org.mp.naumann.processor.handler.database;

import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.Table;
import org.mp.naumann.processor.batch.Batch;

public class PassThroughDatabaseBatchHandler implements DatabaseBatchHandler {

    private final DataConnector dataConnector;

    public PassThroughDatabaseBatchHandler(DataConnector dataConnector) {
        this.dataConnector = dataConnector;
    }

    public void handleBatch(Batch batch) {
        Table table = dataConnector.getTable(batch.getTableName());
        table.execute(batch);
    }
}
