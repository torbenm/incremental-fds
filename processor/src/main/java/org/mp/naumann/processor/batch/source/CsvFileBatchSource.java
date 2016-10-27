package org.mp.naumann.processor.batch.source;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.mp.naumann.database.identifier.DefaultRowIdentifier;
import org.mp.naumann.database.identifier.RowIdentifier;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.processor.batch.Batch;

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
    }


    private void readFile() {
        CSVParser parser = null;
        try {
            parser = CSVParser.parse(csvFile, Charset.defaultCharset(), CSVFormat.DEFAULT);
            parser.getHeaderMap();
            for (CSVRecord csvRecord : parser) {

                Map<String, String> values = csvRecord.toMap();

                String action = values.get(ACTION_COLUMN_NAME);
                int record = Integer.parseInt(values.get(RECORD_COLUMN_NAME));
                RowIdentifier rowId = new DefaultRowIdentifier(record);

                values.remove(ACTION_COLUMN_NAME);
                values.remove("record");

                Statement stmt = createStatement(action, values);
                addStatement(getTableName(), stmt);
            }

        } catch (IOException e) {
            try {
                if (parser != null)
                    parser.close();
            } catch (IOException e1) {
            }
            e.printStackTrace();
        }
    }

    private Statement createStatement(String type, Map values){
        switch(type.toLowerCase()){
            case "insert":
            case "delete":
            case "update":
            default:
                return null; //TODO: need something better here
        }
    }
}
