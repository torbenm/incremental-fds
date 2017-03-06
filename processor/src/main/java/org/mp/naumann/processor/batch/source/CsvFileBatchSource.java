package org.mp.naumann.processor.batch.source;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.mp.naumann.database.statement.DefaultDeleteStatement;
import org.mp.naumann.database.statement.DefaultInsertStatement;
import org.mp.naumann.database.statement.DefaultUpdateStatement;
import org.mp.naumann.database.statement.Statement;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

abstract class CsvFileBatchSource extends AbstractBatchSource {

    private static final Charset CHARSET = Charset.defaultCharset();
    private static final CSVFormat FORMAT = CSVFormat.DEFAULT.withSkipHeaderRecord();
    private static final String ACTION_COLUMN_NAME = "::action";
    private static final String defaultAction = "insert";

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
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String[] header = Arrays.stream(reader.readLine().split(",")).map(String::toLowerCase).toArray(String[]::new);
                    csvParser = CSVParser.parse(file,CHARSET,FORMAT.withHeader(header));
                }
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
            String action = (csvRecord.isSet(ACTION_COLUMN_NAME) ? csvRecord.get(ACTION_COLUMN_NAME) : defaultAction);
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
                    String[] splitValues = value.split("\\|", -1);
                    oldValues.put(key, splitValues[0]);
                    newValues.put(key, splitValues.length > 1 ? splitValues[1] : splitValues[0]);
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
