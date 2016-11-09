package org.mp.naumann.database.jdbc;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.data.Column;
import org.mp.naumann.database.identifier.DefaultRowIdentifier;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class JdbcTableTest extends JdbcTest {

    private static Table table, invalidTable;

    @BeforeClass
    public static void setUpTables() {
        table = connector.getTable(testTableName);
        invalidTable = connector.getTable("invalid");
    }

    @Test
    public void testProperties() {
        assertTrue(table.getName().equals(testTableName));
        assertTrue(table.getRowIdentifierType().equals(DefaultRowIdentifier.class));
    }

    @Test
    public void testGetColumnNames() {
        List<String> columnNames = table.getColumnNames();
        assertTrue(columnNames.size() == 8);
        assertTrue(columnNames.get(0).equals("Numeral"));
        assertTrue(invalidTable.getColumnNames().size() == 0);
    }

    @Test
    public void testGetRowCount() {
        assertTrue(table.getRowCount() == 173);
        assertTrue(invalidTable.getRowCount() == -1);
    }

    @Test
    public void testGetColumn() {
        // retrieve column and check properties
        Column<?> col = table.getColumn("Name");
        assertTrue(col.getName().equals("Name"));
        assertTrue(col.getColumnType() == String.class);

        // get value, make sure the type is what it said it would be
        Object value = col.getValue(new DefaultRowIdentifier(0));
        assertTrue(value.getClass() == String.class);
        assertTrue(value.equals("Moon"));

        // test retrieval of all values
        List<?> values = col.toList();
        assertTrue(values.size() == 173);
        assertTrue(values.get(4).equals("Europa"));

        // check properties of non-existing column
        Column<?> invalidCol = table.getColumn("invalid");
        assertTrue(invalidCol == null);
    }
}
