package org.mp.naumann.processor.batch.source;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.mp.naumann.database.statement.DefaultDeleteStatement;
import org.mp.naumann.database.statement.DefaultInsertStatement;
import org.mp.naumann.database.statement.DefaultUpdateStatement;
import org.mp.naumann.database.statement.Statement;

public class FixedSizeBatchSource extends SizableBatchSource {

	private final File csvFile;

    public FixedSizeBatchSource(String filePath, String schema, String tableName, int batchSize) {
        this(new File(filePath), schema, tableName, batchSize);
    }
    public FixedSizeBatchSource(String filePath, String schema, String tableName, int batchSize, int stopAfter) {
        this(new File(filePath), schema, tableName, batchSize, stopAfter);
    }

    public FixedSizeBatchSource(File file, String schema, String tableName, int batchSize) {
        super(schema, tableName, batchSize);
        this.csvFile = file;
    }

    public FixedSizeBatchSource(File file, String schema, String tableName, int batchSize, int stopAfter) {
        super(schema, tableName, batchSize, stopAfter);
        this.csvFile = file;
    }

	protected void start() {
        parseFile(csvFile);
        finishFilling();
	}

}
