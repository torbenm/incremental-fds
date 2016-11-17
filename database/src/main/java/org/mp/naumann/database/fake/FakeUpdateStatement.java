package org.mp.naumann.database.fake;

import java.util.Map;

import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.UpdateStatement;

public class FakeUpdateStatement implements UpdateStatement {

	@Override
	public Map<String, String> getOldValueMap() {
		return null;
	}

	@Override
	public String getTableName() {
		return null;
	}

	@Override
	public String getSchema() {
		return null;
	}

	@Override
	public boolean isOfEqualLayout(Statement statement) { return false; }

	@Override
	public Map<String, String> getValueMap() {
		return null;
	}
}
