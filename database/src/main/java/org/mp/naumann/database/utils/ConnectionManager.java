package org.mp.naumann.database.utils;

import org.mp.naumann.data.ResourceConnector;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.jdbc.ConnectionInfo;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class ConnectionManager {

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

    private static ConnectionInfo getPostgresConnectionInfo(String db, String user, String pass) throws ConnectionException {
        ConnectionInfo ci = new ConnectionInfo();
        Properties properties = new Properties();

        InputStream input = ConnectionInfo.class.getClassLoader().getResourceAsStream("properties.xml");
        if (input == null)
            input = ConnectionInfo.class.getClassLoader().getResourceAsStream("properties.default.xml");
        if (input != null) {
            try {
                try {
                    properties.loadFromXML(input);
                    String server = properties.getProperty("server");
                    int port = Integer.parseInt(properties.getProperty("port"));
                    String database = (db == null ? properties.getProperty("database") : db);
                    ci.user = (user == null ? properties.getProperty("user") : user);
                    ci.pass = (pass == null ? properties.getProperty("pass") : pass);
                    ci.connectionString = "jdbc:postgresql://" + server + ":" + Integer.toString(port) + "/" + database;
                    return ci;
                } finally {
                    input.close();
                }
            } catch (Exception e) {
                throw new ConnectionException(e);
            }
        } else throw new ConnectionException("Neither default nor custom properties file found.");
    }

    public static Connection getPostgresConnection(String db, String user, String pass) throws ConnectionException {
        return getConnection("org.postgresql.Driver", getPostgresConnectionInfo(db, user, pass));
    }

    public static Connection getCsvConnection(String folder, String separator) throws ConnectionException {
        String resourcePath = ResourceConnector.getResourcePath(folder, "");
        ConnectionInfo ci = new ConnectionInfo();
        ci.connectionString = "jdbc:relique:csv:" + resourcePath + "?separator=" + separator;
        return getConnection("org.relique.jdbc.csv.CsvDriver", ci);
    }

    public static Connection getCsvConnectionFromAbsolutePath(String absoluteFilePath, String separator) throws ConnectionException {
        ConnectionInfo ci = new ConnectionInfo();
        ci.connectionString = "jdbc:relique:csv:" + absoluteFilePath + "?separator=" + separator;
        return getConnection("org.relique.jdbc.csv.CsvDriver", ci);
    }

}
