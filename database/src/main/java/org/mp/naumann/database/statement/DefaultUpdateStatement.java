package org.mp.naumann.database.statement;

import org.mp.naumann.database.identifier.RowIdentifier;

import java.util.Map;

public class DefaultUpdateStatement extends StatementBase implements UpdateStatement {

    public DefaultUpdateStatement(Map<String, String> map, RowIdentifier rowIdentifier, String tableName) {
        super(map, rowIdentifier, tableName);
    }
}
