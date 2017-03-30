package org.mp.naumann.processor.batch.source;

import org.mp.naumann.processor.batch.Batch;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractBatchSource implements BatchSource {

    private final Set<BatchSourceListener> batchSourceListenerSet = new HashSet<>();

    public void addBatchSourceListener(BatchSourceListener batchSourceListener) {
        batchSourceListenerSet.add(batchSourceListener);
    }

    public void removeBatchSourceListener(BatchSourceListener batchSourceListener) {
        batchSourceListenerSet.remove(batchSourceListener);
    }

    Set<BatchSourceListener> getBatchSourceListener() {
        return batchSourceListenerSet;
    }

    protected void notifyListener(Batch batch) {
        for (BatchSourceListener listener : getBatchSourceListener())
            listener.batchArrived(batch);
    }
}
