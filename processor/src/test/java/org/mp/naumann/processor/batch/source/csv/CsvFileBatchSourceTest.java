package org.mp.naumann.processor.batch.source.csv;

import org.junit.Test;
import org.mp.naumann.database.statement.DeleteStatement;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.UpdateStatement;
import org.mp.naumann.processor.batch.source.AbstractBatchSourceTest;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mp.naumann.processor.batch.source.csv.CsvKeyWord.ACTION_COLUMN;
import static org.mp.naumann.processor.batch.source.csv.CsvKeyWord.DELETE_STATEMENT;
import static org.mp.naumann.processor.batch.source.csv.CsvKeyWord.INSERT_STATEMENT;
import static org.mp.naumann.processor.batch.source.csv.CsvKeyWord.UPDATE_STATEMENT;

public class CsvFileBatchSourceTest extends AbstractBatchSourceTest {

    protected final String SCHEMA = "SCHEMA";
    protected final String TABLE = "TABLE";

    protected  final CsvFileBatchSource csvFileBatchSource;

    private final Map<String, String> expectedOldValues = new HashMap<>();
    private final Map<String, String> expectedNewValues = new HashMap<>();

    {
        expectedOldValues.put("1", "eins");
        expectedOldValues.put("2", "zwei");
        expectedOldValues.put("3", "drei");
        expectedNewValues.put("1", "oans");
        expectedNewValues.put("2", "zwoa");
        expectedNewValues.put("3", "droa");
    }




    public CsvFileBatchSourceTest(){
        this.csvFileBatchSource = new CsvFileBatchSource(SCHEMA, TABLE) {
            @Override
            void addStatement(Statement stmt) {
                // Empty add Statement - we do not really want to add statements.
            }
        };
        this.abstractBatchSource = csvFileBatchSource;
    }

    @Test
    public void test_parse_record_insert(){
        Map<String, String> values = addStatementType(INSERT_STATEMENT, createValueMap());

        Statement c = csvFileBatchSource.parseRecord(values);
        assertTrue(c instanceof InsertStatement);
        assertEquals(c.getSchema(), SCHEMA);
        assertEquals(c.getTableName(), TABLE);
        assertEquals(((InsertStatement)c).getValueMap(), createValueMap());
    }

    @Test
    public void test_parse_record_delete(){
        Map<String, String> values = addStatementType(DELETE_STATEMENT, createValueMap());

        Statement c = csvFileBatchSource.parseRecord(values);
        assertTrue(c instanceof DeleteStatement);
        assertEquals(c.getSchema(), SCHEMA);
        assertEquals(c.getTableName(), TABLE);
        assertEquals(((DeleteStatement)c).getValueMap(), createValueMap());
    }

    @Test
    public void test_parse_record_update(){
        Map<String, String> values = addStatementType(UPDATE_STATEMENT, createUpdateMap());

        Statement c = csvFileBatchSource.parseRecord(values);

        assertTrue(c instanceof UpdateStatement);
        assertEquals(c.getSchema(), SCHEMA);
        assertEquals(c.getTableName(), TABLE);
        UpdateStatement us = (UpdateStatement)c;
        assertEquals(us.getOldValueMap(), expectedOldValues);
        assertEquals(us.getNewValueMap(), expectedNewValues);

    }


    @Test
    public void test_create_insert_statement(){
        Map<String, String> valueMap = createValueMap();
        Statement c = csvFileBatchSource.createStatement(
                CsvKeyWord.INSERT_STATEMENT.getKeyWord(),
                valueMap
        );
        assertTrue(c instanceof InsertStatement);
        assertEquals(c.getSchema(), SCHEMA);
        assertEquals(c.getTableName(), TABLE);
        assertEquals(((InsertStatement)c).getValueMap(), valueMap);
    }

    @Test
    public void test_create_delete_statement(){
        Map<String, String> valueMap = createValueMap();
        Statement c = csvFileBatchSource.createStatement(
                CsvKeyWord.DELETE_STATEMENT.getKeyWord(),
                valueMap
        );
        assertTrue(c instanceof DeleteStatement);
        assertEquals(c.getSchema(), SCHEMA);
        assertEquals(c.getTableName(), TABLE);
        assertEquals(((DeleteStatement)c).getValueMap(), valueMap);
    }

    @Test
    public void test_create_update_statement(){
        Map<String, String> valueMap = createUpdateMap();

        Statement c = csvFileBatchSource.createStatement(
                CsvKeyWord.UPDATE_STATEMENT.getKeyWord(),
                valueMap
        );
        assertTrue(c instanceof UpdateStatement);
        assertEquals(c.getSchema(), SCHEMA);
        assertEquals(c.getTableName(), TABLE);
        UpdateStatement us = (UpdateStatement)c;
        assertEquals(us.getOldValueMap(), expectedOldValues);
        assertEquals(us.getNewValueMap(), expectedNewValues);

    }


    private Map<String, String> createValueMap(){
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put("1", "eins");
        valueMap.put("2", "zwei");
        valueMap.put("3", "drei");
        return valueMap;
    }

    private Map<String, String> createUpdateMap(){
        Map<String, String> valueMap =new HashMap<>();
        valueMap.put("1", "eins|oans");
        valueMap.put("2", "zwei|zwoa");
        valueMap.put("3", "drei|droa");
        return valueMap;
    }

    private Map<String, String> addStatementType(CsvKeyWord keyWord, Map<String, String> valueMap){
        valueMap.put(ACTION_COLUMN.getKeyWord(), keyWord.getKeyWord());
        return valueMap;
    }
}
