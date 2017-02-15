package org.mp.naumann.database.fake;

import org.mp.naumann.database.statement.InsertStatement;

import java.util.Map;

public class FakeInsertStatement implements InsertStatement {

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
