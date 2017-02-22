package org.mp.naumann.processor.batch.source.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.mp.naumann.database.statement.DefaultDeleteStatement;
import org.mp.naumann.database.statement.DefaultInsertStatement;
import org.mp.naumann.database.statement.DefaultUpdateStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.processor.batch.source.AbstractBatchSource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mp.naumann.processor.batch.source.csv.CsvKeyWord.*;

public abstract class CsvFileBatchSource extends AbstractBatchSource {

    private static final Charset CHARSET = Charset.defaultCharset();
    private static final CSVFormat FORMAT = CSVFormat.DEFAULT.withFirstRecordAsHeader().withNullString("");
    private static final CsvKeyWord defaultAction = CsvKeyWord.INSERT_STATEMENT;

    final List<Statement> statementList = new ArrayList<>();
    final String schema;
    final String tableName;

    private CSVParser csvParser;
    private String filename;

    CsvFileBatchSource(String schema, String tableName) {
        this.schema = schema;
        this.tableName = tableName;
    }

    private CSVParser initializeCsvParser(File file) {
        if ((csvParser == null) || (!file.getAbsolutePath().equals(filename))) {
            try {
                csvParser = CSVParser.parse(file, CHARSET, FORMAT);
                filename = file.getAbsolutePath();
            } catch (IOException e) {
                //
            }
        }
        return csvParser;
    }

    abstract void addStatement(Statement stmt);

    void parseFile(File file) {
        parseRecords(initializeCsvParser(file));
    }

    void parseRecords(CSVParser csvParser){
        for(CSVRecord record : csvParser){
            addStatement(parseRecord(record));
        }
    }
    Statement parseRecord(CSVRecord csvRecord){
        return parseRecord(csvRecord.toMap());
    }

    Statement parseRecord(Map<String, String> values){
        String action = (values.containsKey(ACTION_COLUMN.getKeyWord())
                ? values.get(ACTION_COLUMN.getKeyWord()) : defaultAction.getKeyWord());

        values.remove(ACTION_COLUMN.getKeyWord());
        Statement stmt = createStatement(action, values);
        return stmt;
    }

    private String sanitize(String value) {
        return (value.isEmpty() ? null : value);
    }

    Statement createStatement(String type, Map<String, String> values) {
        switch (CsvKeyWord.valueOfKeyWord(type)) {
            case INSERT_STATEMENT:
                return new DefaultInsertStatement(values, schema, tableName);
            case DELETE_STATEMENT:
                return new DefaultDeleteStatement(values, schema, tableName);
            case UPDATE_STATEMENT:
                Map<String, String> oldValues = new HashMap<>();
                Map<String, String> newValues = new HashMap<>();
                values.forEach((key, value) -> {
                    String[] splitValues = value.split("\\|", -1);
                    oldValues.put(key, sanitize(splitValues[0]));
                    newValues.put(key, sanitize(splitValues.length > 1 ? splitValues[1] : splitValues[0]));
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
                .filter(s -> !(s.equals(ACTION_COLUMN.getKeyWord())))
                .collect(Collectors.toList());
    }
}
