package org.mp.naumann.database.statement;

import org.mp.naumann.database.identifier.RowIdentifier;

import java.util.Map;

public class DefaultUpdateStatement extends StatementBase implements UpdateStatement {

    private final Map<String, String> oldValueMap;

    public DefaultUpdateStatement(Map<String, String> map, Map<String, String> oldValueMap, RowIdentifier rowIdentifier, String tableName) {
        super(map, rowIdentifier, tableName);
        this.oldValueMap = oldValueMap;
    }

    public Map<String, String> getOldValueMap() {
        return oldValueMap;
    }
}
