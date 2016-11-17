package org.mp.naumann.database.statement;

import java.util.Map;

public class DefaultInsertStatement extends StatementBase implements InsertStatement {

    public DefaultInsertStatement(Map<String, String> map, String schema, String tableName) {
        super(map, schema, tableName);
    }

    @Override
    public boolean isOfEqualLayout(Statement statement) {
        return
                statement.getClass().equals(this.getClass()) &&
                statement.getTableName().equalsIgnoreCase(this.getTableName()) &&
                statement.getSchema().equalsIgnoreCase(this.getSchema()) &&
                this.getValueMap().size() == statement.getValueMap().size() &&
                this.getValueMap().keySet().equals(statement.getValueMap().keySet());
    }
}
