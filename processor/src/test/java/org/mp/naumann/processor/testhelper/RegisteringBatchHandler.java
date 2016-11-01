package org.mp.naumann.processor.testhelper;

import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.handler.BatchHandler;

public class RegisteringBatchHandler implements BatchHandler {
    private boolean reached = false;
    @Override
    public void handleBatch(Batch batch) {
        reached = true;
    }
    public boolean isReached() {
        return reached;
    }
}