package org.mp.naumann.database.jdbc;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.utils.ConnectionManager;

import java.sql.Connection;

import static org.junit.Assert.assertEquals;

public class ConnectionManagerTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testGetCsvConnection() throws ConnectionException {
        Connection conn = ConnectionManager.getCsvConnection("", ";");
        JdbcDataConnector connector = new JdbcDataConnector(conn);
        Table table = connector.getTable("test", "countries");
        assertEquals(248, table.getRowCount());
        assertEquals(17, table.getColumns().size());
    }

    @Test
    public void testGetCsvConnectionInvalidPath() throws ConnectionException {
        thrown.expect(ConnectionException.class);
        ConnectionManager.getCsvConnection("/test");
    }

    @Test
    public void testGetCsvConnectionWrongSeparator() throws ConnectionException {
        Connection conn = ConnectionManager.getCsvConnection();
        JdbcDataConnector connector = new JdbcDataConnector(conn);
        thrown.expect(RuntimeException.class);
        connector.getTable("test", "countries").getColumnNames();
    }
}
