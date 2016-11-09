package org.mp.naumann.processor.fake;

import org.mp.naumann.processor.batch.source.SizableBatchSource;

public class FakeSizeableBatchSource extends SizableBatchSource{

    public FakeSizeableBatchSource(String tableName, int batchSize) {
        super(tableName, batchSize);
    }


}
