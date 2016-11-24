package org.mp.naumann.database.fake;

import java.util.List;

import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.Table;

public class FakeDataConnector implements DataConnector {
    @Override
    public List<String> getTableNames(String schema) {
        return null;
    }

    @Override
    public Table getTable(String schema, String tableName) {
        return new FakeTable();
    }

    @Override
    public void close() {

    }
}
