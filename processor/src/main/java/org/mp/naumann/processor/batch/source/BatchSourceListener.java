package org.mp.naumann.processor.batch.source;

import org.mp.naumann.processor.batch.Batch;

import java.util.EventListener;

public interface BatchSourceListener extends EventListener {

    void batchArrived(Batch batch);
}
