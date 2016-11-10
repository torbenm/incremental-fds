package org.mp.naumann.database.jdbc;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.utils.PostgresConnection;

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
		assertEquals(table.getName(), tableName);
	}

	@Rule
	public ExpectedException thrown = ExpectedException.none();

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

	@Test
	public void testMissingDriver() throws ClassNotFoundException, ConnectionException {
		thrown.expect(ClassNotFoundException.class);
		try (DataConnector conn = new JdbcDataConnector("org.postgresql.Driver2",
				PostgresConnection.getConnectionInfo())) {

		}
	}
}
