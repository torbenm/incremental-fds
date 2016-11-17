package org.mp.naumann.database.fake;

import java.util.Map;

import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.Statement;

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
    public Map<String, String> getValueMap() {
        return null;
    }

    @Override
    public boolean isOfEqualLayout(Statement statement) { return false; }

}
