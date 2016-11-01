package org.mp.naumann.processor.testhelper;

import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;

public class MakeSureImLastDatabaseBatchHandler implements DatabaseBatchHandler {

    @Override
    public void handleBatch(Batch batch) {
        throw new DatabaseHandlerEnteredException();
    }

    public static class DatabaseHandlerEnteredException extends RuntimeException{}
}
