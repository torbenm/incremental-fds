package org.mp.naumann.database.fake;

import org.mp.naumann.database.statement.UpdateStatement;

import java.util.Map;

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
	public boolean isEmpty() {
		return false;
	}

	@Override
	public Map<String, String> getNewValueMap() {
		return null;
	}
}
