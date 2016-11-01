package org.mp.naumann.database.fake;

import org.mp.naumann.database.identifier.RowIdentifier;
import org.mp.naumann.database.statement.DeleteStatement;

import java.util.Map;

public class FakeDeleteStatement implements DeleteStatement {
    @Override
    public RowIdentifier getRowIdentifier() {
        return null;
    }

    @Override
    public String getTableName() {
        return null;
    }

    @Override
    public Map<String, String> getValueMap() {
        return null;
    }
}
