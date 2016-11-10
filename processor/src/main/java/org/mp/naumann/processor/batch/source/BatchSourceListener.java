package org.mp.naumann.processor.batch.source;

import java.util.EventListener;

import org.mp.naumann.processor.batch.Batch;

public interface BatchSourceListener extends EventListener {

    void batchArrived(Batch batch);
}
