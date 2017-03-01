package org.mp.naumann.testcases;

import ResourceConnection.ResourceConnector;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.processor.batch.source.FixedSizeBatchSource;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;

public class SingleFileTestCase extends BaseTestCase {

    private final int splitLine;
    private final int batchSize;

    public SingleFileTestCase(String tableName, IncrementalFDConfiguration config, int stopAfter, boolean hyfdOnly, int splitLine, int batchSize) {
        super("", tableName, config, stopAfter, hyfdOnly);
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