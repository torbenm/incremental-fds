package org.mp.naumann.database.jdbc;

import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.Table;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcDataConnector implements DataConnector {

    private Connection conn;
    private Map<String, List<String>> tableNames = new HashMap<>();

    public JdbcDataConnector(Connection connection) {
        this.conn = connection;
    }

    public List<String> getTableNames(String schema) {
        if (!tableNames.containsKey(schema)) {
            List<String> result = new ArrayList<>();
            try {
                DatabaseMetaData md = conn.getMetaData();
                try (ResultSet rs = md.getTables(null, schema, "%", null)) {
                    while (rs.next()) {
                        String name = rs.getString(3);
                        if (name.startsWith(schema + "."))
                            name = name.substring(schema.length() + 1);
                        result.add(name);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            tableNames.put(schema, result);
            return result;
        }
        return tableNames.get(schema);
    }

    public void clearTableNames(String schema) {
        tableNames.remove(schema);
    }

    public Table getTable(String schema, String tableName) {
        if (!getTableNames(schema).contains(tableName))
            throw new RuntimeException(String.format("Table '%s' not found in schema '%s'", tableName, schema));
        return new JdbcTable(schema, tableName, conn);
    }

    public void close() throws ConnectionException {
        try {
            conn.close();
        } catch (SQLException e) {
            throw new ConnectionException(e);
        }
    }
}
