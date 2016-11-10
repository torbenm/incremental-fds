package org.mp.naumann.database.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.NoSuchElementException;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mp.naumann.database.InputReadException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.Column;

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
		assertEquals(col.getType(), String.class);

		// check properties of non-existing column
		Column<?> invalidCol = table.getColumn("invalid");
		assertEquals(invalidCol, null);
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
		assertEquals(173, i);
	}

	@Test
	public void testWrongInputIteration() throws InputReadException {
		try (TableInput input = table.open()) {
			for (int i = 0; i < 174; i++) {
				input.next();
			}
		} catch (NoSuchElementException e) {
			return;
		}
		fail();
	}
}
