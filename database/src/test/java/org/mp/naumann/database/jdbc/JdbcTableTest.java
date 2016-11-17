package org.mp.naumann.database.jdbc;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mp.naumann.database.InputReadException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.Column;

import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;

public class JdbcTableTest extends JdbcTest {

	private static Table table, invalidTable;

	@BeforeClass
	public static void setUpTables() {
		table = connector.getTable(schema, tableName);
		invalidTable = connector.getTable("", "invalid");
	}

	@Test
	public void testProperties() {
		assertEquals(tableName, table.getName());
	}

	@Test
	public void testGetColumnNames() {
		List<String> columnNames = table.getColumnNames();
		assertEquals(17, columnNames.size());
		assertEquals("country_en", columnNames.get(0));
		assertEquals(0, invalidTable.getColumnNames().size());
	}

	@Test
	public void testGetRowCount() {
		assertEquals(248, table.getRowCount());
		assertEquals(-1, invalidTable.getRowCount());
	}

	@Test
	public void testGetColumn() {
		// retrieve column and check properties
		Column<?> col = table.getColumn("country_en");
		assertEquals("country_en", col.getName());
		assertEquals(String.class, col.getType());

		// check properties of non-existing column
		Column<?> invalidCol = table.getColumn("invalid");
		assertEquals(null, invalidCol);
	}

	@Test
	public void testExecute() {
		// not implemented atm because the JDBC CSV driver only supports SELECT
	}

	@Test
	public void testInput() throws InputReadException {
		int i = 0;
		try (TableInput input = table.open()) {
			while (input.hasNext()) {
				input.next();
				i++;
			}
		}
		assertEquals(248, i);
	}

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testWrongInputIteration() throws InputReadException {
		thrown.expect(NoSuchElementException.class);
		try (TableInput input = table.open()) {
			for (int i = 0; i < 249; i++) {
				input.next();
			}
		}
	}

}
