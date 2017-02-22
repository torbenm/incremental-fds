package org.mp.naumann.testcases;

import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.batch.source.csv.VariableSizeCsvBatchSource;

public class VariableSizeTestCase extends BaseTestCase {

    private final String directory;

    public VariableSizeTestCase(String tableName, IncrementalFDConfiguration config, int stopAfter, boolean hyfdOnly, String directory) {
        super("", tableName, config, stopAfter, hyfdOnly);
        this.directory = directory;
    }

    protected String getBatchSize() {
        return "variable";
    }

    protected StreamableBatchSource getBatchSource() {
        return new VariableSizeCsvBatchSource(schema, tableName, directory + sourceTableName);
    }

}
