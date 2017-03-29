package org.mp.naumann.database.fake;

import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.Table;

import java.util.List;

public class FakeDataConnector implements DataConnector {

    public List<String> getTableNames(String schema) {
        return null;
    }

    public Table getTable(String schema, String tableName) {
        return new FakeTable();
    }

    public void clearTableNames(String schema) {
    }

    public void close() {
    }
}
