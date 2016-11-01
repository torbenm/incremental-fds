package org.mp.naumann.processor;

import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.fake.FakeBatch;
import org.mp.naumann.processor.fake.FakeBatchHandler;
import org.mp.naumann.processor.handler.BatchHandler;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class BatchProcessorTests {

    public static void testBatchHandlerCollection(BatchProcessor processor){
        BatchHandler bh = new FakeBatchHandler();
        processor.addBatchHandler(bh);
        assertTrue(processor.getBatchHandlers().contains(bh));
        processor.removeBatchHandler(bh);
        assertFalse(processor.getBatchHandlers().contains(bh));
    }


    public static void testDistributeBatch(BatchProcessor processor){
        BatchHandler bh = new BatchHandler() {
            @Override
            public void handleBatch(Batch batch) {
                throw new RuntimeException();
            }
        };
        processor.addBatchHandler(bh);
        processor.batchArrived(new FakeBatch());
    }
}
