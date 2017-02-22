package org.mp.naumann.processor.handler.database;

import org.junit.Rule;
import org.junit.rules.ExpectedException;

public class PassThroughDatabaseBatchHandlerTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();
	
   /* @Test
	public void testHandle(){
        // Must throw exception when method is reached        
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Cannot execute StatementGroup");
        
        PassThroughDatabaseBatchHandler ptdbh = new PassThroughDatabaseBatchHandler(new FakeDataConnector());
        ptdbh.handleBatch(new FakeBatch());
    } */
}
