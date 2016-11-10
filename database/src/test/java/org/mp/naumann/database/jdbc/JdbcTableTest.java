package org.mp.naumann.database.jdbc;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.data.Column;
import org.mp.naumann.database.identifier.DefaultRowIdentifier;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class JdbcTableTest extends JdbcTest {

    private static Table table, invalidTable;

    @BeforeClass
    public static void setUpTables() {
        table = connector.getTable(testTableName);
        invalidTable = connector.getTable("invalid");
    }

    @Test
    public void testProperties() {
        assertEquals(table.getName(), testTableName);
        assertEquals(table.getRowIdentifierType(), DefaultRowIdentifier.class);
    }

    @Test
    public void testGetColumnNames() {
        List<String> columnNames = table.getColumnNames();
        assertEquals(columnNames.size(), 8);
        assertEquals(columnNames.get(0), "Numeral");
        assertEquals(invalidTable.getColumnNames().size(), 0);
    }

    @Test
    public void testGetRowCount() {
        assertEquals(table.getRowCount(), 173);
        assertEquals(invalidTable.getRowCount(), -1);
    }

    @Test
    public void testGetColumn() {
        // retrieve column and check properties
        Column<?> col = table.getColumn("Name");
        assertEquals(col.getName(), "Name");
        assertEquals(col.getColumnType(), String.class);

        // get value, make sure the type is what it said it would be
        Object value = col.getValue(new DefaultRowIdentifier(0));
        assertEquals(value.getClass(), String.class);
        assertEquals(value, "Moon");

        // test retrieval of all values
        List<?> values = col.toList();
        assertEquals(values.size(), 173);
        assertEquals(values.get(4), "Europa");

        // check properties of non-existing column
        Column<?> invalidCol = table.getColumn("invalid");
        assertEquals(invalidCol, null);
    }
}
