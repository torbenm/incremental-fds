package org.mp.naumann.database.fake;

import java.util.List;

import org.mp.naumann.database.InputReadException;
import org.mp.naumann.database.Table;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.Column;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.StatementGroup;

public class FakeTable implements Table {

	@Override
	public String getName() {
		return null;
	}

	@Override
	public List<Column<String>> getColumns() {
		return null;
	}

	@Override
	public long getRowCount() {
		return 0;
	}

	@Override
	public boolean execute(Statement statement) {
		return false;
	}

	@Override
	public boolean execute(StatementGroup statementGroup) {
		throw new RuntimeException("Cannot execute StatementGroup");
	}

	@Override
	public TableInput open() throws InputReadException {
		return null;
	}
}
