package org.mp.naumann.database.fake;

import org.mp.naumann.database.statement.DeleteStatement;

import java.util.Map;

public class FakeDeleteStatement implements DeleteStatement {

    @Override
    public String getTableName() {
        return null;
    }

    @Override
    public String getSchema() {
        return null;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Map<String, String> getValueMap() {
        return null;
    }

}
