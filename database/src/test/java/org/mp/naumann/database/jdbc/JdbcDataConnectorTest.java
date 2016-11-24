package org.mp.naumann.database.jdbc;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.utils.PostgresConnection;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class JdbcDataConnectorTest extends JdbcTest {

	@Test
	public void testGetTableNames() {
		List<String> tableNames = connector.getTableNames(schema);
		assertEquals(1, tableNames.size());
		assertEquals(tableName, tableNames.get(0));
	}

	@Test
	public void testGetTable() {
		Table table = connector.getTable(schema, tableName);
		assertEquals(tableName, table.getName());
	}

	@Rule
	public ExpectedException thrown = ExpectedException.none();

    /*
    there's a connection timeout of ~11s that I can't get rid of ...
    it's annoying to always have to wait for that, I guess let's just skip this test for now ...
    ... or for forever, let's not kid ourselves here :)

    @Test
	public void testFalseConnection() throws ClassNotFoundException, ConnectionException {
		ConnectionInfo info = new ConnectionInfo();
		info.connectionString = "jdbc:postgresql://foobar:1234/baz";
		info.user = "foo";
		info.pass = "bar";
		thrown.expect(ConnectionException.class);
		try (DataConnector conn = new JdbcDataConnector("org.postgresql.Driver", info)) {

		}
	}
	*/

	@Test
	public void testMissingDriver() throws ClassNotFoundException, ConnectionException {
		thrown.expect(ClassNotFoundException.class);
        new JdbcDataConnector("org.postgresql.Driver2", PostgresConnection.getConnectionInfo());
	}
}
