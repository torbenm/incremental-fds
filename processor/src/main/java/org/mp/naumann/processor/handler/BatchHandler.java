package org.mp.naumann.processor.handler;

import org.mp.naumann.processor.batch.Batch;

public interface BatchHandler {

    void handleBatch(Batch batch);
}
