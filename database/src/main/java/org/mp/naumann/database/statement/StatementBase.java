package org.mp.naumann.database.statement;

import java.sql.JDBCType;
import java.util.HashMap;
import java.util.Map;

abstract class StatementBase implements Statement {

    private final Map<String, JDBCType> jdbcTypes;
    private final String schema;
    private final String tableName;

    StatementBase(Map<String, String> map, Map<String, JDBCType> jdbcTypes, String schema, String tableName) {
        /*
		 * Copy to HashMap for same order of keys in all statements.
		 */
        this.schema = schema;
        this.tableName = tableName;

        if (jdbcTypes == null) {
            this.jdbcTypes = new HashMap<>();
            for (String key : map.keySet()) this.jdbcTypes.put(key, JDBCType.VARCHAR);
        } else this.jdbcTypes = jdbcTypes;
    }

    StatementBase(Map<String, String> map, String schema, String tableName) {
        this(map, null, schema, tableName);
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
    public JDBCType getJDBCType(String columnName) {
        return jdbcTypes.get(columnName);
    }
}
