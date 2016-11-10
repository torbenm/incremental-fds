package org.mp.naumann.database.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.Table;

public class JdbcDataConnector implements DataConnector {

    private Connection conn;
    private ConnectionInfo ci;

    public JdbcDataConnector(String className, ConnectionInfo connectionInfo) throws ClassNotFoundException, ConnectionException {
        Class.forName(className);
        this.ci = connectionInfo;
        connect();
    }

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

    public void connect() throws ConnectionException {
        try {
            conn = DriverManager.getConnection(ci.connectionString, ci.user, ci.pass);
        } catch (SQLException e) {
            throw new ConnectionException(e);
        }
    }

    public void close() throws ConnectionException {
        try {
			conn.close();
		} catch (SQLException e) {
			throw new ConnectionException(e);
		}
    }
}
