package org.mp.naumann.database.statement;

import java.util.HashMap;
import java.util.Map;

class StatementBase implements Statement {

	private final Map<String, String> map;
	private final String schema;
	private final String tableName;

	StatementBase(Map<String, String> map, String schema, String tableName) {
		/*
		 * Copy to HashMap for same order of keys in all statements.
		 */
		this.map = new HashMap<>(map);
		this.schema = schema;
		this.tableName = tableName;
	}

	@Override
	public String getTableName() {
		return tableName;
	}

	@Override
	public String getSchema() {
		return schema;
	}

	@Override
	public Map<String, String> getValueMap() {
		return map;
	}
}
