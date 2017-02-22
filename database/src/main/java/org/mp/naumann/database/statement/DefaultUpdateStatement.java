package org.mp.naumann.database.statement;

import java.sql.JDBCType;
import java.util.Map;

public class DefaultUpdateStatement extends StatementBase implements UpdateStatement {

    private final Map<String, String> oldValueMap;
    private final Map<String, String> newValueMap;

    public DefaultUpdateStatement(Map<String, String> map, Map<String, String> oldValueMap,
                                  Map<String, JDBCType> jdbcTypes, String schema, String tableName) {
        super(map, jdbcTypes, schema, tableName);
        this.newValueMap = map;
        this.oldValueMap = oldValueMap;
    }

    public DefaultUpdateStatement(Map<String, String> map, Map<String, String> oldValueMap, String schema,
                                  String tableName) {
        super(map, schema, tableName);
        this.newValueMap = map;
        this.oldValueMap = oldValueMap;
    }

    public Map<String, String> getOldValueMap() {
        return oldValueMap;
    }

    @Override
    public Map<String, String> getNewValueMap() {
        return newValueMap;
    }

    @Override
    public boolean isEmpty() {
        return newValueMap.isEmpty() || oldValueMap.isEmpty();
    }

}
