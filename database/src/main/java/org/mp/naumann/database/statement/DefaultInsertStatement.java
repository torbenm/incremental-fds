package org.mp.naumann.database.statement;

import org.mp.naumann.database.identifier.RowIdentifier;

import java.util.Map;

public class DefaultInsertStatement extends StatementBase implements InsertStatement {


    public DefaultInsertStatement(Map<String, String> map, RowIdentifier rowIdentifier, String tableName) {
        super(map, rowIdentifier, tableName);
    }
}
