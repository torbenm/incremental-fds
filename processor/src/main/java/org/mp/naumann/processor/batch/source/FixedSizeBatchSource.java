package org.mp.naumann.processor.batch.source;

import java.io.File;

public class FixedSizeBatchSource extends SizableBatchSource {

	private final File csvFile;

    public FixedSizeBatchSource(String fileName, String schema, String tableName, int batchSize) {
        super(schema, tableName, batchSize);
        this.csvFile = new File(fileName);
    }
    public FixedSizeBatchSource(String fileName, String schema, String tableName, int batchSize, int stopAfter, int skipFirst) {
        super(schema, tableName, batchSize, stopAfter, skipFirst);
        this.csvFile = new File(fileName);
    }

	protected void start() {
        parseFile(csvFile);
        finishFilling();
	}

}
