package org.mp.naumann.database.statement;

import org.mp.naumann.database.identifier.RowIdentifier;

import java.util.HashMap;
import java.util.Map;

class StatementBase implements Statement {

    private final Map<String, String> map;
    private final RowIdentifier rowIdentifier;
    private final String tableName;

    StatementBase(Map<String, String> map, RowIdentifier rowIdentifier, String tableName) {
        /* Copy to HashMap for same order of keys in all statements.
         */
        this.map = new HashMap<>(map);
        this.rowIdentifier = rowIdentifier;
        this.tableName = tableName;
    }

    public Map<String, String> getValueMap() {
        return map;
    }

    public RowIdentifier getRowIdentifier() {
        return rowIdentifier;
    }

    public String getTableName() {
        return tableName;
    }
}
