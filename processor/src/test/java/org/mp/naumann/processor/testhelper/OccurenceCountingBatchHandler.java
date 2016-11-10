package org.mp.naumann.processor.testhelper;

import static org.junit.Assert.assertEquals;

import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.handler.BatchHandler;

public class OccurenceCountingBatchHandler implements BatchHandler {
    private static int num = 0;
    private final int occurance;

    public OccurenceCountingBatchHandler(int occurance) {
        this.occurance = occurance;
    }

    @Override
    public void handleBatch(Batch batch) {
        assertEquals(num, occurance);
        num++;
    }

    public static void reset(){
        num = 0;
    }
}
