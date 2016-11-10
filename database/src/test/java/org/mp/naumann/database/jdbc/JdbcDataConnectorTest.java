package org.mp.naumann.database.jdbc;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;
import org.mp.naumann.database.Table;

public class JdbcDataConnectorTest extends JdbcTest {

    @Test
    public void testGetTableNames() {
        List<String> tableNames = connector.getTableNames(schema);
        assertEquals(1, tableNames.size());
        assertEquals(testTableName, tableNames.get(0));
    }

    @Test
    public void testGetTable() {
        Table table = connector.getTable(schema, testTableName);
        assertEquals(table.getName(), testTableName);
    }
}

