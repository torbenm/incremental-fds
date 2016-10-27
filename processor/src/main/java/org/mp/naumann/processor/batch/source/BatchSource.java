package org.mp.naumann.processor.batch.source;

import org.mp.naumann.processor.batch.Batch;

import java.util.Iterator;

public interface BatchSource {

    void addBatchSourceListener(BatchSourceListener batchSourceListener);
    void removeBatchSourceListener(BatchSourceListener batchSourceListener);
}