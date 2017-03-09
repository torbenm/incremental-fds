package org.mp.naumann.testcases;

import ResourceConnection.ResourceConnector;
import org.mp.naumann.processor.batch.source.FixedSizeBatchSource;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;

import static java.lang.Double.NaN;

public class FixedSizeTestCase extends BaseTestCase {

    private int batchSize;
    private final double batchSizeRatio;

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
        return new FixedSizeBatchSource(ResourceConnector.FULL_BATCHES + sourceTableName + ".csv", schema, tableName, batchSize, stopAfter, 0);
    }

    @Override
    protected void setBaselineSize(long baselineSize) {
        super.setBaselineSize(baselineSize);
        if (batchSizeRatio != NaN)
            batchSize = Math.max((int) Math.round(baselineSize * batchSizeRatio), 1);
    }

    @Override
    protected String getBatchSize() {
        if (batchSizeRatio != NaN)
            return Double.toString(batchSizeRatio);
        else
            return Integer.toString(batchSize);
    }

}
