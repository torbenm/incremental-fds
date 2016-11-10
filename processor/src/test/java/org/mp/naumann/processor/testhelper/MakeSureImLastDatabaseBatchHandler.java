package org.mp.naumann.processor.testhelper;

import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.fake.FakeDataConnector;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;

public class MakeSureImLastDatabaseBatchHandler implements DatabaseBatchHandler {

    @Override
    public void handleBatch(Batch batch) {
        throw new DatabaseHandlerEnteredException();
    }

    public static class DatabaseHandlerEnteredException extends RuntimeException{

		private static final long serialVersionUID = -8850815656670068379L;}

	@Override
	public DataConnector getConnector() {
		return new FakeDataConnector();
	}
}
