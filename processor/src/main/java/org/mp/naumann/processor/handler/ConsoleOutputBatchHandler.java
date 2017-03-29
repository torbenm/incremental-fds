package org.mp.naumann.processor.handler;

import org.mp.naumann.processor.batch.Batch;

/**
 * A BatchHandler that outputs information of the batch on the console.
 */
public class ConsoleOutputBatchHandler implements BatchHandler {
    @Override
    public void handleBatch(Batch batch) {
        System.out.println("Received batch of size " + batch.getSize());
        System.out.println("----- Insert statements: " + batch.getInsertStatements().size());
        System.out.println("----- Delete statements: " + batch.getDeleteStatements().size());
        System.out.println("----- Update statements: " + batch.getUpdateStatements().size());
    }
}
