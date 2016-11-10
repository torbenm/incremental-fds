package org.mp.naumann.processor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.mp.naumann.processor.fake.FakeBatch;
import org.mp.naumann.processor.fake.FakeBatchHandler;
import org.mp.naumann.processor.handler.BatchHandler;
import org.mp.naumann.processor.testhelper.BatchArrivedException;
import org.mp.naumann.processor.testhelper.RegisteringBatchHandler;
import org.mp.naumann.processor.testhelper.MakeSureImLastDatabaseBatchHandler;


class BatchProcessorTests {

    void testBatchHandlerCollection(BatchProcessor processor){
        BatchHandler bh = new FakeBatchHandler();
        processor.addBatchHandler(bh);
        assertTrue(processor.getBatchHandlers().contains(bh));
        processor.removeBatchHandler(bh);
        assertFalse(processor.getBatchHandlers().contains(bh));
    }


    void testDistributeBatch(BatchProcessor processor){
        processor.addBatchHandler((Batch) -> { throw new BatchArrivedException(); });
        processor.batchArrived(new FakeBatch());
    }

    long testDistributeToMultipleBatches(BatchProcessor batchProcessor, int i) {
       for(int j = 0; j < i;j++){
           batchProcessor.addBatchHandler(new RegisteringBatchHandler());
       }
       batchProcessor.batchArrived(new FakeBatch());
        return batchProcessor.getBatchHandlers().parallelStream()
                .filter(n -> ((RegisteringBatchHandler)n).isReached())
                .count();
    }

    boolean testDatabaseHandlerIsLast(BatchProcessor batchProcessor, int i){
        if(!(batchProcessor.getDatabaseBatchHandler() instanceof MakeSureImLastDatabaseBatchHandler))
            throw new AssertionError("BatchProcessor has wrong DatabaseBatchHandler assigned!");
        for(int j = 0; j < i;j++){
            batchProcessor.addBatchHandler(new RegisteringBatchHandler());
        }
        try {
            batchProcessor.batchArrived(new FakeBatch());
        }catch(MakeSureImLastDatabaseBatchHandler.DatabaseHandlerEnteredException e){
            return batchProcessor.getBatchHandlers().parallelStream()
                    .filter(n -> ((RegisteringBatchHandler)n).isReached())
                    .count() == i;
        }
        return false;
    }

}
