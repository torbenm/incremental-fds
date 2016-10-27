package org.mp.naumann.database;

import java.util.List;

public interface DataConnector {

    List<String> getTableNames();
    Table getTable(String tableName);

    void connect();
    void disconnect();

}
