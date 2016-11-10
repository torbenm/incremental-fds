package org.mp.naumann.database.fake;

import java.util.Map;

import org.mp.naumann.database.statement.DeleteStatement;

public class FakeDeleteStatement implements DeleteStatement {

    @Override
    public String getTableName() {
        return null;
    }

    @Override
    public Map<String, String> getValueMap() {
        return null;
    }
}
