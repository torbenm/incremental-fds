package org.mp.naumann.database.statement;

import org.mp.naumann.database.identifier.RowIdentifier;

public class DefaultDeleteStatement implements DeleteStatement {

    private final RowIdentifier rowIdentifier;
    private final String tableName;

    public DefaultDeleteStatement(RowIdentifier rowIdentifier, String tableName) {
        this.rowIdentifier = rowIdentifier;
        this.tableName = tableName;
    }

    @Override
    public RowIdentifier getRowIdentifier() {
        return rowIdentifier;
    }

    @Override
    public String getTableName() {
        return tableName;
    }
}
