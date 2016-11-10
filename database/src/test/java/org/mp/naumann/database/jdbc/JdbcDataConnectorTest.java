package org.mp.naumann.database.jdbc;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;
import org.mp.naumann.database.Table;

public class JdbcDataConnectorTest extends JdbcTest {

    @Test
    public void testGetTableNames() {
        List<String> tableNames = connector.getTableNames();
        assertEquals(tableNames.size(), 1);
        assertEquals(tableNames.get(0), testTableName);
    }

    @Test
    public void testGetTable() {
        Table table = connector.getTable(testTableName);
        assertEquals(table.getName(), testTableName);
    }
}

