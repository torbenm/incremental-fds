package org.mp.naumann.database.fake;

import org.mp.naumann.database.identifier.RowIdentifier;
import org.mp.naumann.database.statement.UpdateStatement;

import java.util.Map;

public class FakeUpdateStatement implements UpdateStatement {
    @Override
    public RowIdentifier getRowIdentifier() {
        return null;
    }

    @Override
    public Map<String, String> getOldValueMap() {
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
