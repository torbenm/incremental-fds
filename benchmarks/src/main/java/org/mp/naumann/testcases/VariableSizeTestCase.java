package org.mp.naumann.testcases;

import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.batch.source.csv.VariableSizeCsvBatchSource;

public class VariableSizeTestCase extends BaseTestCase {

    private final String directory;

    public VariableSizeTestCase(TestCaseParameters parameters, String directory) {
        super(parameters);
        this.directory = directory;
    }

    protected String getBatchSize() {
        return "variable";
    }

    protected StreamableBatchSource getBatchSource() {
        return new VariableSizeCsvBatchSource(schema, tableName, directory + sourceTableName);
    }

}
