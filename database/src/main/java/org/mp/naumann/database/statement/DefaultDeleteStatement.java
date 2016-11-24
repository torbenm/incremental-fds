package org.mp.naumann.database.statement;

import java.util.Map;

public class DefaultDeleteStatement extends StatementBase implements DeleteStatement {

    public DefaultDeleteStatement(Map<String, String> map, String schema, String tableName) {
        super(map, schema, tableName);
    }

    @Override
    public boolean isOfEqualLayout(Statement statement){
        return
                statement.getClass().equals(this.getClass()) &&
                this.getTableName().equalsIgnoreCase(statement.getTableName()) &&
                this.getSchema().equalsIgnoreCase(statement.getSchema());
    }

}
