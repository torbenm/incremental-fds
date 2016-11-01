package org.mp.naumann.processor.fake;

import org.mp.naumann.database.DataConnector;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;


public class FakeDatabaseBatchHandler implements DatabaseBatchHandler {

    @Override
    public void handleBatch(Batch batch) {
        // do nothing
    }
}
