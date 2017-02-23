package org.mp.naumann.testcases;

import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.data.ResourceConnector;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.batch.source.csv.FixedSizeCsvBatchSource;

public class FixedSizeTestCase extends BaseTestCase {

    private final int batchSize;

    public FixedSizeTestCase(String tableName, IncrementalFDConfiguration config, int stopAfter, boolean hyfdOnly, int batchSize) {
        super("", tableName, config, stopAfter, hyfdOnly);
        this.batchSize = batchSize;
    }

    protected String getBatchSize() {
        return Integer.toString(batchSize);
    }

    protected StreamableBatchSource getBatchSource() {
        return new FixedSizeCsvBatchSource(ResourceConnector.FULL_BATCHES + sourceTableName + ".csv", schema, tableName, batchSize, stopAfter, 0);
    }

}
