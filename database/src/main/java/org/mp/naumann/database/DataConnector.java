package org.mp.naumann.database;

import java.util.List;

public interface DataConnector extends AutoCloseable {

    List<String> getTableNames(String schema);
    Table getTable(String schema, String tableName);

    void close() throws ConnectionException;

}
