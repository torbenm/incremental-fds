package org.mp.naumann.database.statement;

import org.mp.naumann.database.identifier.RowIdentifier;

import java.util.Map;

public class DefaultDeleteStatement extends StatementBase implements DeleteStatement {

    public DefaultDeleteStatement(Map<String, String> map, RowIdentifier rowIdentifier, String tableName) {
        super(map, rowIdentifier, tableName);
    }

}
