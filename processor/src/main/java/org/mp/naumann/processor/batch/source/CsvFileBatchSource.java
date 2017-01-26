package org.mp.naumann.processor.batch.source;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.mp.naumann.database.statement.DefaultDeleteStatement;
import org.mp.naumann.database.statement.DefaultInsertStatement;
import org.mp.naumann.database.statement.DefaultUpdateStatement;
import org.mp.naumann.database.statement.Statement;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class CsvFileBatchSource extends AbstractBatchSource {

    private static final Charset CHARSET = Charset.defaultCharset();
    private static final CSVFormat FORMAT = CSVFormat.DEFAULT.withFirstRecordAsHeader();
    public static final String ACTION_COLUMN_NAME = "::action";

    final List<Statement> statementList = new ArrayList<>();
    final String schema;
    final String tableName;

    private CSVParser csvParser;
    private String filename;

    CsvFileBatchSource(String schema, String tableName) {
        this.schema = schema;
        this.tableName = tableName;
    }

    private void initializeCsvParser(File file) {
        if ((csvParser == null) || (!file.getAbsolutePath().equals(filename))) {
            try {
                csvParser = CSVParser.parse(file, CHARSET, FORMAT);
                filename = file.getAbsolutePath();
            } catch (IOException e) {
                //
            }
        }
    }

    abstract void addStatement(Statement stmt);

    void parseFile(File file) {
        initializeCsvParser(file);
        for (CSVRecord csvRecord : csvParser) {
            Map<String, String> values = csvRecord.toMap();

            String action = csvRecord.get(ACTION_COLUMN_NAME);

            values.remove(ACTION_COLUMN_NAME);
            Statement stmt = createStatement(action, values);
            addStatement(stmt);
        }
    }

    private Statement createStatement(String type, Map<String, String> values) {
        switch (type.toLowerCase()) {
            case "insert":
                return new DefaultInsertStatement(values, schema, tableName);
            case "delete":
                return new DefaultDeleteStatement(values, schema, tableName);
            case "update":
                Map<String, String> oldValues = new HashMap<>();
                Map<String, String> newValues = new HashMap<>();
                values.forEach((key, value) -> {
                    oldValues.put(key, value.split("\\|")[0]);
                    newValues.put(key, value.split("\\|")[1]);
                });
                return new DefaultUpdateStatement(newValues, oldValues, schema, tableName);
            default:
                throw new RuntimeException(String.format("Illegal statement type: %s", type));
        }
    }

    public List<String> getColumnNames(){
        return csvParser.getHeaderMap()
                .entrySet()
                .parallelStream()
                .sorted(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .filter(s -> !(s.equals(ACTION_COLUMN_NAME)))
                .collect(Collectors.toList());
    }
}
