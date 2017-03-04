package org.mp.naumann.testcases;

import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.batch.source.VariableSizeBatchSource;

public class VariableSizeTestCase extends BaseTestCase {

    private final String directory;

    public VariableSizeTestCase(String tableName, IncrementalFDConfiguration config, int stopAfter, boolean hyfdOnly, boolean hyfdCreateIndex, String directory) {
        super("", tableName, config, stopAfter, hyfdOnly, hyfdCreateIndex);
        this.directory = directory;
    }

    protected String getBatchSize() {
        return "variable";
    }

    protected StreamableBatchSource getBatchSource() {
        return new VariableSizeBatchSource(schema, tableName, directory + sourceTableName);
    }

}
