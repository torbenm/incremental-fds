package org.mp.naumann.processor.handler;

import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.exceptions.BatchHandlingException;

public interface BatchHandler {

    void handleBatch(Batch batch);
}
