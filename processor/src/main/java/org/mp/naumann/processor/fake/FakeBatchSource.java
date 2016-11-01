package org.mp.naumann.processor.fake;

import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.source.AbstractBatchSource;

public class FakeBatchSource extends AbstractBatchSource {


    public void send(Batch batch){
        this.notifyListener(batch);
    }

}
