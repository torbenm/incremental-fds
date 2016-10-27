package org.mp.naumann.processor.handler;

import org.mp.naumann.processor.batch.Batch;

public class ConsoleOutputBatchHandler implements BatchHandler {
    @Override
    public void handleBatch(Batch batch) {
        System.out.println("Received batch of size "+ batch.getSize());
    }
}
