package org.mp.naumann.testcases;

import ResourceConnection.ResourceConnector;
import org.mp.naumann.processor.batch.source.FixedSizeBatchSource;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;

public class FixedSizeTestCase extends BaseTestCase {

    private final int batchSize;

    public FixedSizeTestCase(TestCaseParameters parameters, int batchSize) {
        super(parameters);
        this.batchSize = batchSize;
    }

    protected String getBatchSize() {
        return Integer.toString(batchSize);
    }

    protected StreamableBatchSource getBatchSource() {
        return new FixedSizeBatchSource(ResourceConnector.FULL_BATCHES + sourceTableName + ".csv", schema, tableName, batchSize, stopAfter, 0);
    }

}
