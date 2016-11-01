package org.mp.naumann.processor;

import org.junit.Before;
import org.junit.Test;
import org.mp.naumann.processor.fake.FakeBatchSource;
import org.mp.naumann.processor.fake.FakeDatabaseBatchHandler;

public class SynchronousBatchProcessorTests {

    private BatchProcessor batchProcessor;

    @Before
    public void init(){
        batchProcessor = new SynchronousBatchProcessor(new FakeBatchSource(), new FakeDatabaseBatchHandler());
    }

    @Test(expected=RuntimeException.class)
    public void testDistributeBatch(){
        BatchProcessorTests.testDistributeBatch(batchProcessor);
    }

    @Test
    public void testBatchHandlerCollection(){
        BatchProcessorTests.testBatchHandlerCollection(batchProcessor);
    }
}
