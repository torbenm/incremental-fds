package org.mp.naumann.database.statement;

import java.sql.JDBCType;
import java.util.HashMap;
import java.util.Map;

public class DefaultInsertStatement extends StatementBase implements InsertStatement {

    private final Map<String, String> map;

    public DefaultInsertStatement(Map<String, String> map, Map<String, JDBCType> jdbcTypes, String schema, String tableName) {
        super(map, jdbcTypes, schema, tableName);
        this.map = new HashMap<>(map);
    }

    public DefaultInsertStatement(Map<String, String> map, String schema, String tableName) {
        super(map, schema, tableName);
        this.map = new HashMap<>(map);
    }

    @Override
    public boolean isOfEqualLayout(Statement statement) {
        if (!(statement instanceof InsertStatement)) {
            return false;
        }
        InsertStatement insert = (InsertStatement) statement;
        return
                insert.getClass().equals(this.getClass()) &&
                        insert.getTableName().equalsIgnoreCase(this.getTableName()) &&
                        insert.getSchema().equalsIgnoreCase(this.getSchema()) &&
                        this.getValueMap().size() == insert.getValueMap().size() &&
                        this.getValueMap().keySet().equals(insert.getValueMap().keySet());
    }

    @Override
    public Map<String, String> getValueMap() {
        return map;
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }
}
