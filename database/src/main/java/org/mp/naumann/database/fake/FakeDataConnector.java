package org.mp.naumann.database.fake;

import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.Table;

import java.util.List;

public class FakeDataConnector implements DataConnector {
    @Override
    public List<String> getTableNames() {
        return null;
    }

    @Override
    public Table getTable(String tableName) {
        return new FakeTable();
    }

    @Override
    public void connect() {

    }

    @Override
    public void disconnect() {

    }
}
