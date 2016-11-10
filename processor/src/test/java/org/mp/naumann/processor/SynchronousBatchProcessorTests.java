package org.mp.naumann.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.mp.naumann.processor.fake.FakeBatch;
import org.mp.naumann.processor.fake.FakeBatchSource;
import org.mp.naumann.processor.fake.FakeDatabaseBatchHandler;
import org.mp.naumann.processor.testhelper.BatchArrivedException;
import org.mp.naumann.processor.testhelper.MakeSureImLastDatabaseBatchHandler;
import org.mp.naumann.processor.testhelper.OccurenceCountingBatchHandler;

public class SynchronousBatchProcessorTests extends BatchProcessorTests {

    private BatchProcessor batchProcessor;

    @Before
    public void init(){
        batchProcessor = new SynchronousBatchProcessor(new FakeBatchSource(), new FakeDatabaseBatchHandler());
    }

    @Test(expected=BatchArrivedException.class)
    public void testDistributeBatch(){
        testDistributeBatch(batchProcessor);
    }

    @Test
    public void testBatchHandlerCollection(){
        testBatchHandlerCollection(batchProcessor);
    }

    @Test
    public void testMultipleBatchDistribution(){
        assertEquals(testDistributeToMultipleBatches(batchProcessor, 10), 10L);
        assertEquals(testDistributeToMultipleBatches(batchProcessor, 5), 15L);
    }

    @Test
    public void testSynchronicity(){
        OccurenceCountingBatchHandler.reset();
        for(int i = 0; i < 10; i++){
            batchProcessor.addBatchHandler(new OccurenceCountingBatchHandler(i));
        }
        batchProcessor.batchArrived(new FakeBatch());
    }

    @Test
    public void testDatabaseHandlerIsLast(){
        BatchProcessor bp = new SynchronousBatchProcessor(new FakeBatchSource(), new MakeSureImLastDatabaseBatchHandler());
        assertTrue(testDatabaseHandlerIsLast(bp, 10));
    }



}
