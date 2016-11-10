package org.mp.naumann.processor.handler.database;

import org.junit.Test;
import org.mp.naumann.processor.fake.FakeBatch;
import org.mp.naumann.database.fake.FakeDataConnector;

public class PassThroughDatabaseBatchHandlerTest {

    @Test(expected=RuntimeException.class)
    public void testHandle(){
        // Must throw exception when method is reached
        PassThroughDatabaseBatchHandler ptdbh = new PassThroughDatabaseBatchHandler(new FakeDataConnector());
        ptdbh.handleBatch(new FakeBatch());
    }
}
