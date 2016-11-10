package org.mp.naumann.database;

import java.util.List;

public interface DataConnector {

    List<String> getTableNames(String schema);
    Table getTable(String schema, String tableName);

    void connect();
    void disconnect();

}
