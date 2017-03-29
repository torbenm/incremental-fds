package org.mp.naumann.database.statement;

import java.sql.JDBCType;
import java.util.Map;

public class DefaultDeleteStatement extends StatementBase implements DeleteStatement {

    private final Map<String, String> map;

    public DefaultDeleteStatement(Map<String, String> map, Map<String, JDBCType> jdbcTypes, String schema, String tableName) {
        super(map, jdbcTypes, schema, tableName);
        this.map = map;
    }

    public DefaultDeleteStatement(Map<String, String> map, String schema, String tableName) {
        super(map, schema, tableName);
        this.map = map;
    }

    @Override
    public boolean isOfEqualLayout(Statement statement) {
        return
                statement.getClass().equals(this.getClass()) &&
                        this.getTableName().equalsIgnoreCase(statement.getTableName()) &&
                        this.getSchema().equalsIgnoreCase(statement.getSchema());
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public Map<String, String> getValueMap() {
        return map;
    }
}
