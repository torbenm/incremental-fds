package org.mp.naumann.database.statement;

import java.util.Map;

public class DefaultDeleteStatement extends StatementBase implements DeleteStatement {

    public DefaultDeleteStatement(Map<String, String> map, String schema, String tableName) {
        super(map, schema, tableName);
    }

}
