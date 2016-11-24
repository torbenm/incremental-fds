package org.mp.naumann.database.utils;

import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.jdbc.ConnectionInfo;

import java.sql.Connection;
import java.sql.DriverManager;

public class ConnectionManager {

    private static final String defaultCsvSeparator = ",";
    private static final String defaultCsvDir = ".";

    private static Connection getConnection(String className, ConnectionInfo ci) throws ConnectionException {
        Connection connection;
        try {
            Class.forName(className);
            connection = DriverManager.getConnection(ci.connectionString, ci.user, ci.pass);
        } catch (Exception e) {
            throw new ConnectionException(e);
        }
        return connection;
    }

    public static Connection getPostgresConnection() throws ConnectionException {
        return getConnection("org.postgresql.Driver", PostgresConnection.getConnectionInfo());
    }

    public static Connection getCsvConnection(String csvDir, String separator) throws ConnectionException {
        ConnectionInfo ci = new ConnectionInfo();
        ci.connectionString = "jdbc:relique:csv:" + csvDir + "?separator=" + separator;
        return getConnection("org.relique.jdbc.csv.CsvDriver", ci);
    }

    public static Connection getCsvConnection(String csvDir) throws ConnectionException {
        return getCsvConnection(csvDir, defaultCsvSeparator);
    }

    public static Connection getCsvConnection() throws ConnectionException {
        return getCsvConnection(defaultCsvDir, defaultCsvSeparator);
    }

}
