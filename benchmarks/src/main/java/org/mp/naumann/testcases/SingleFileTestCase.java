package org.mp.naumann.testcases;

import ResourceConnection.ResourceConnector;
import org.mp.naumann.processor.batch.source.FixedSizeBatchSource;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;

public class SingleFileTestCase extends BaseTestCase {

    private final int splitLine;
    private final int batchSize;

    public SingleFileTestCase(TestCaseParameters parameters, int splitLine, int batchSize) {
        super(parameters);
        this.splitLine = splitLine;
        this.batchSize = batchSize;
    }

    @Override
    int getLimit() { return splitLine; }

    protected String getBatchSize() { return Integer.toString(batchSize); }

    protected StreamableBatchSource getBatchSource() {
        return new FixedSizeBatchSource(ResourceConnector.BASELINE + sourceTableName + ".csv", schema, tableName, batchSize, stopAfter, splitLine);
    }

}