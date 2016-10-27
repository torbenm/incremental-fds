package org.mp.naumann.database;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCDataConnector implements DataConnector {

    private Connection conn;

    public JDBCDataConnector(String className, String connectionString) {
        try {
            Class.forName(className);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public List<String> getTableNames() {
        List<String> result = new ArrayList<>();
        try {
            DatabaseMetaData md = conn.getMetaData();
            ResultSet rs = md.getTables(null, null, "%", null);
            while (rs.next()) {
                result.add(rs.getString(3));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public Table getTable(String tableName) {
        return null;
    }

    public void connect(String connectionString) {
        try {
            conn = DriverManager.getConnection(connectionString);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void disconnect() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
