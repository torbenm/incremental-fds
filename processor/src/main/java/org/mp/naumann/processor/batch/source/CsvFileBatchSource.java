package org.mp.naumann.processor.batch.source;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.mp.naumann.database.identifier.DefaultRowIdentifier;
import org.mp.naumann.database.identifier.RowIdentifier;
import org.mp.naumann.database.statement.DefaultDeleteStatement;
import org.mp.naumann.database.statement.DefaultInsertStatement;
import org.mp.naumann.database.statement.DefaultUpdateStatement;
import org.mp.naumann.database.statement.Statement;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

public class CsvFileBatchSource extends SizableBatchSource {

    private final File csvFile;

    private static final String ACTION_COLUMN_NAME = "::action";
    private static final String RECORD_COLUMN_NAME = "::record";

    public CsvFileBatchSource(String filePath, String tableName, int batchSize) {
        this(new File(filePath), tableName, batchSize);
    }

    public CsvFileBatchSource(File file, String tableName, int batchSize) {
        super(tableName, batchSize);
        this.csvFile = file;
        readFile();
    }


    private void readFile() {
        CSVParser parser = null;
        try {
            parser = CSVParser.parse(csvFile, Charset.defaultCharset(), CSVFormat.DEFAULT.withFirstRecordAsHeader());

            for (CSVRecord csvRecord : parser) {
                Map<String, String> values = csvRecord.toMap();

                String action = csvRecord.get(ACTION_COLUMN_NAME);
                int record = Integer.parseInt(csvRecord.get(RECORD_COLUMN_NAME));
                RowIdentifier rowId = new DefaultRowIdentifier(record);

                values.remove(ACTION_COLUMN_NAME);
                values.remove(RECORD_COLUMN_NAME);

                Statement stmt = createStatement(action, values, rowId);
                addStatement(getTableName(), stmt);
            }
            finishFilling();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Statement createStatement(String type, Map<String, String> values, RowIdentifier rowIdentifier){
        switch(type.toLowerCase()){
            case "insert":
                return new DefaultInsertStatement(values, rowIdentifier, this.getTableName());
            case "delete":
                return new DefaultDeleteStatement(values, rowIdentifier, this.getTableName());
            case "update":
                return new DefaultUpdateStatement(values, rowIdentifier, this.getTableName());
            default:
                return null; //TODO: need something better here
        }
    }
}
