package org.mp.naumann.testcases;

import org.mp.naumann.data.ResourceConnector;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.batch.source.csv.FixedSizeCsvBatchSource;

import static java.lang.Double.NaN;

public class FixedSizeTestCase extends BaseTestCase {

    private final double batchSizeRatio;
    private int batchSize;

    public FixedSizeTestCase(TestCaseParameters parameters, int batchSize) {
        super(parameters);
        this.batchSize = batchSize;
        this.batchSizeRatio = NaN;
    }

    public FixedSizeTestCase(TestCaseParameters parameters, double batchSizeRatio) {
        super(parameters);
        this.batchSize = 0;
        this.batchSizeRatio = batchSizeRatio;
    }

    protected StreamableBatchSource getBatchSource() {
        return new FixedSizeCsvBatchSource(ResourceConnector.FULL_BATCHES + sourceTableName + ".csv", schema, tableName, batchSize, stopAfter, 0);
    }

    @Override
    protected void setBaselineSize(long baselineSize) {
        super.setBaselineSize(baselineSize);
        if (batchSizeRatio != NaN)
            batchSize = Math.max((int) Math.round(baselineSize * batchSizeRatio), 1);
    }

    protected String getBatchSize() {
        return Integer.toString(batchSize);
    }
}
