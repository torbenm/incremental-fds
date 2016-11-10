package org.mp.naumann.database.statement;

import java.util.Map;

public class DefaultInsertStatement extends StatementBase implements InsertStatement {

    public DefaultInsertStatement(Map<String, String> map, String tableName) {
        super(map, tableName);
    }

    @Override
    public boolean isOfEqualSchema(InsertStatement statement) {
        return statement.getTableName().equalsIgnoreCase(this.getTableName()) &&
            this.getValueMap().size() == statement.getValueMap().size() &&
                this.getValueMap().keySet().equals(statement.getValueMap().keySet());
    }
}
