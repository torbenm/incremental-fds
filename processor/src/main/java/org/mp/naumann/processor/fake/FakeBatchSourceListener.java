package org.mp.naumann.processor.fake;

import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.source.BatchSourceListener;

public class FakeBatchSourceListener implements BatchSourceListener {

    boolean reached = false;
    @Override
    public void batchArrived(Batch batch) {
        reached = true;
    }

    public boolean isReached() {
        return reached;
    }
}
