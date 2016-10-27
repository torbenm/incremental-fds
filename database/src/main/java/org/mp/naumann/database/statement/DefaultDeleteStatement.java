package org.mp.naumann.database.statement;

import org.mp.naumann.database.identifier.RowIdentifier;

public class DefaultDeleteStatement implements DeleteStatement {

    private final RowIdentifier rowIdentifier;

    public DefaultDeleteStatement(RowIdentifier rowIdentifier) {
        this.rowIdentifier = rowIdentifier;
    }

    @Override
    public RowIdentifier getRowIdentifier() {
        return rowIdentifier;
    }
}
