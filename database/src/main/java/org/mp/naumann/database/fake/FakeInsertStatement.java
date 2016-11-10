package org.mp.naumann.database.fake;

import org.mp.naumann.database.identifier.RowIdentifier;
import org.mp.naumann.database.statement.InsertStatement;

import java.util.Map;

public class FakeInsertStatement implements InsertStatement {
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

    @Override
    public boolean isOfEqualSchema(InsertStatement statement) {
        return false;
    }
}
