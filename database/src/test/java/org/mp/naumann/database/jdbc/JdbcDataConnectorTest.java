package org.mp.naumann.database.jdbc;

import org.junit.Test;
import org.mp.naumann.database.Table;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class JdbcDataConnectorTest extends JdbcTest {



    @Test
    public void testGetTableNames() {
        List<String> tableNames = connector.getTableNames();
        assertTrue(tableNames.size() == 1);
        assertTrue(tableNames.get(0).equals(testTableName));
    }

    @Test
    public void testGetTable() {
        Table table = connector.getTable(testTableName);
        assertTrue(table.getName().equals(testTableName));
    }
}

