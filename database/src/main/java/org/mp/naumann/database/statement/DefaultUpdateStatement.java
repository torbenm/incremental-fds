package org.mp.naumann.database.statement;

import java.util.Map;

public class DefaultUpdateStatement extends StatementBase implements UpdateStatement {

    private final Map<String, String> oldValueMap;

    public DefaultUpdateStatement(Map<String, String> map, Map<String, String> oldValueMap, String tableName) {
        super(map, tableName);
        this.oldValueMap = oldValueMap;
    }

    public Map<String, String> getOldValueMap() {
        return oldValueMap;
    }


}
