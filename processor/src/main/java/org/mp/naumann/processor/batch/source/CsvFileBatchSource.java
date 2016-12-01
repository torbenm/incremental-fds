package org.mp.naumann.processor.batch.source;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.mp.naumann.database.statement.DefaultDeleteStatement;
import org.mp.naumann.database.statement.DefaultInsertStatement;
import org.mp.naumann.database.statement.DefaultUpdateStatement;
import org.mp.naumann.database.statement.Statement;

public class CsvFileBatchSource extends SizableBatchSource {

	private static final Charset CHARSET = Charset.defaultCharset();

	private static final CSVFormat FORMAT = CSVFormat.DEFAULT.withFirstRecordAsHeader();
	private final File csvFile;

	private static final String ACTION_COLUMN_NAME = "::action";
	private static final String RECORD_COLUMN_NAME = "::record";
	
    public CsvFileBatchSource(String filePath, String schema, String tableName, int batchSize) {
        this(new File(filePath), schema, tableName, batchSize);
    }

    public CsvFileBatchSource(File file, String schema, String tableName, int batchSize) {
        super(schema, tableName, batchSize);
        this.csvFile = file;
    }

	protected void start() {
		try (CSVParser parser = CSVParser.parse(csvFile, CHARSET, FORMAT)) {

			for (CSVRecord csvRecord : parser) {
				Map<String, String> values = csvRecord.toMap();

				String action = csvRecord.get(ACTION_COLUMN_NAME);

				values.remove(ACTION_COLUMN_NAME);
				values.remove(RECORD_COLUMN_NAME);
				Statement stmt = createStatement(action, values);
				addStatement(getTableName(), stmt);
			}
			finishFilling();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected Statement createStatement(String type, Map<String, String> values) {
		switch (type.toLowerCase()) {
		case "insert":
			return new DefaultInsertStatement(values, this.getSchema(), this.getTableName());
		case "delete":
			return new DefaultDeleteStatement(values, this.getSchema(), this.getTableName());
		case "update":
			Map<String, String> oldValues = new HashMap<>();
			Map<String, String> newValues = new HashMap<>();
			values.forEach((key, value) -> {
				oldValues.put(key, value.split("\\|")[0]);
				newValues.put(key, value.split("\\|")[1]);
			});
			return new DefaultUpdateStatement(newValues, oldValues, this.getSchema(), this.getTableName());
		default:
			return null; // TODO: need something better here
		}
	}
}
