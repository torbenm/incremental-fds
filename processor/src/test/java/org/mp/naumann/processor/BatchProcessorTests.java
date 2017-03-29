package org.mp.naumann.processor;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.source.BatchSource;
import org.mp.naumann.processor.handler.BatchHandler;
import org.mp.naumann.processor.handler.DataAwareBatchHandler;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public abstract class BatchProcessorTests {

    @Mock
    protected BatchHandler batchHandler;

    @Mock
    protected BatchSource batchSource;

    @Mock
    protected DatabaseBatchHandler databaseBatchHandler;

    protected int numberOfDistributedBatchHandlers = 20;

    protected BatchProcessor batchProcessor;

    public BatchProcessorTests() {
        MockitoAnnotations.initMocks(this);
    }

    @Before
    public void setup() {
        batchProcessor.addBatchHandler(batchHandler);
    }

    @Test
    public void testBatchHandlerCollection() {
        BatchHandler bh = mock(BatchHandler.class);
        batchProcessor.addBatchHandler(bh);
        assertTrue(batchProcessor.getBatchHandlers().contains(bh));
        batchProcessor.removeBatchHandler(bh);
        assertFalse(batchProcessor.getBatchHandlers().contains(bh));
    }

    @Test
    public void test_add_dataware_batch_handler() {
        DataAwareBatchHandler dabh = mock(DataAwareBatchHandler.class);
        batchProcessor.addDataAwareBatchHandler(dabh);
        verify(dabh, times(1)).setDataConnector(any(DataConnector.class));
        assertTrue(batchProcessor.getBatchHandlers().contains(dabh));
        batchProcessor.removeBatchHandler(dabh);
        assertFalse(batchProcessor.getBatchHandlers().contains(dabh));
    }

    @Test
    public void test_distribute_batch() {
        Batch batch = mock(Batch.class);
        batchProcessor.batchArrived(batch);
        verify(batchHandler).handleBatch(batch);
    }

    @Test
    public void test_distribute_to_multiple_batches() {
        BatchHandler bh2 = mock(BatchHandler.class);
        batchProcessor.addBatchHandler(bh2);

        Batch batch = mock(Batch.class);
        batchProcessor.batchArrived(batch);
        verify(batchHandler).handleBatch(batch);
        verify(bh2).handleBatch(batch);

        batchProcessor.removeBatchHandler(bh2);
    }

    @Test
    public void test_distribute_to_n_batches() {
        List<BatchHandler> batchHandlers = new ArrayList<>(numberOfDistributedBatchHandlers);
        for (int i = 0; i < numberOfDistributedBatchHandlers; i++) {
            batchHandlers.add(mock(BatchHandler.class));
            batchProcessor.addBatchHandler(batchHandlers.get(i));
        }
        Batch batch = mock(Batch.class);
        batchProcessor.batchArrived(batch);
        batchHandlers.stream().map(Mockito::verify).forEach(b -> b.handleBatch(batch));
        batchHandlers.forEach(batchProcessor::removeBatchHandler);
    }

    @Test
    public void test_database_handler_is_first() {
        batchProcessor.setInsertToDatabaseFirst(true);
        Batch batch = mock(Batch.class);
        Mockito.doThrow(new BatchHandledException()).when(databaseBatchHandler).handleBatch(batch);
        try {
            batchProcessor.batchArrived(batch);
            fail();
        } catch (BatchHandledException e) {
            verify(batchHandler, never()).handleBatch(batch);
        }
    }

    @Test
    public void test_database_handler_is_last() {
        batchProcessor.setInsertToDatabaseFirst(false);
        Batch batch = mock(Batch.class);
        Mockito.doThrow(new BatchHandledException()).when(databaseBatchHandler).handleBatch(batch);
        try {
            batchProcessor.batchArrived(batch);
            fail();
        } catch (BatchHandledException e) {
            verify(batchHandler).handleBatch(batch);
        }
    }

    private class BatchHandledException extends RuntimeException {
    }
}
