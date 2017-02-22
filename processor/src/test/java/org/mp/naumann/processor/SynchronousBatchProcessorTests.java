package org.mp.naumann.processor;

import org.junit.Test;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.testhelper.OccurenceCountingBatchHandler;

import static org.mockito.Mockito.mock;

public class SynchronousBatchProcessorTests extends BatchProcessorTests {

    public SynchronousBatchProcessorTests(){
        batchProcessor = new SynchronousBatchProcessor(batchSource, databaseBatchHandler);
    }

    @Test
    public void testSynchronicity(){
        OccurenceCountingBatchHandler.reset();
        for(int i = 0; i < numberOfDistributedBatchHandlers; i++){
            batchProcessor.addBatchHandler(new OccurenceCountingBatchHandler(i));
        }
        batchProcessor.batchArrived(mock(Batch.class));
    }

}
