package org.mp.naumann.testcases;

import ResourceConnection.ResourceConnector;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.batch.source.VariableSizeBatchSource;

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
        return new VariableSizeBatchSource(schema, tableName, directory + sourceTableName);
    }

}
