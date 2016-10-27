package org.mp.naumann.processor.handler;

import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.Table;
import org.mp.naumann.processor.batch.Batch;

public class DatabaseBatchHandler implements BatchHandler {

    private final DataConnector dataConnector;

    public DatabaseBatchHandler(DataConnector dataConnector) {
        this.dataConnector = dataConnector;
    }

    public void handleBatch(Batch batch) {
        Table table = dataConnector.getTable(batch.getTableName());
        table.execute(batch);
    }
}
