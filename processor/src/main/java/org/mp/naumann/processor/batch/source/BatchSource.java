package org.mp.naumann.processor.batch.source;

public interface BatchSource {

    void addBatchSourceListener(BatchSourceListener batchSourceListener);

    void removeBatchSourceListener(BatchSourceListener batchSourceListener);
}
