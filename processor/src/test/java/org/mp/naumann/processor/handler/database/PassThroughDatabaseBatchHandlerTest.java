package org.mp.naumann.processor.handler.database;

import org.junit.Test;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.Table;
import org.mp.naumann.processor.batch.Batch;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PassThroughDatabaseBatchHandlerTest {

    @Test
	public void test_handle_calls_table_execute(){
        DataConnector dc = mock(DataConnector.class);
        Table t = mock(Table.class);
        Batch b = mock(Batch.class);

        when(dc.getTable(anyString(), anyString())).thenReturn(t);
        PassThroughDatabaseBatchHandler ptdbh = new PassThroughDatabaseBatchHandler(dc);
        ptdbh.handleBatch(b);
        verify(t).execute(b);
    }



}
