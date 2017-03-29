package org.mp.naumann.processor.batch.source.csv;

import java.io.File;

public class FixedSizeCsvBatchSource extends SizableCsvBatchSource {

    private final File csvFile;

    public FixedSizeCsvBatchSource(String fileName, String schema, String tableName, int batchSize) {
        super(schema, tableName, batchSize);
        this.csvFile = new File(fileName);
    }

    public FixedSizeCsvBatchSource(String fileName, String schema, String tableName, int batchSize, int stopAfter, int skipFirst) {
        super(schema, tableName, batchSize, stopAfter, skipFirst);
        this.csvFile = new File(fileName);
    }

    protected void start() {
        parseFile(csvFile);
        finishFilling();
    }

}
