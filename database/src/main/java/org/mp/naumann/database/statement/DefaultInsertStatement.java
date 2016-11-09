package org.mp.naumann.database.statement;

import org.mp.naumann.database.identifier.RowIdentifier;

import java.util.Map;

public class DefaultInsertStatement extends StatementBase implements InsertStatement {

    public DefaultInsertStatement(Map<String, String> map, RowIdentifier rowIdentifier, String tableName) {
        super(map, rowIdentifier, tableName);
    }

    @Override
    public boolean isOfEqualSchema(InsertStatement statement) {
        return statement.getTableName().equalsIgnoreCase(this.getTableName()) &&
            this.getValueMap().size() == statement.getValueMap().size() &&
                this.getValueMap().keySet().equals(statement.getValueMap().keySet());
    }
}
