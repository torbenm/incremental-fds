package org.mp.naumann.database.jdbc;

import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.Table;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class JdbcDataConnector implements DataConnector {

    private Connection conn;

    public JdbcDataConnector(Connection connection) {
        this.conn = connection;
    }

    public List<String> getTableNames(String schema) {
        List<String> result = new ArrayList<>();
        try {
            DatabaseMetaData md = conn.getMetaData();
            try (ResultSet rs = md.getTables(null, schema, "%", null)) {
                while (rs.next()) {
                    result.add(rs.getString(3));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public Table getTable(String schema, String tableName) {
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
