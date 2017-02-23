package org.mp.naumann.database.utils;

import org.mp.naumann.data.ResourceConnector;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.jdbc.ConnectionInfo;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
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

    private static ConnectionInfo getPostgresConnectionInfo() throws ConnectionException {
        ConnectionInfo ci = new ConnectionInfo();
        Properties properties = new Properties();
        URL settingsURL = ConnectionInfo.class.getClassLoader().getResource("properties.xml");
        if (settingsURL == null) settingsURL = ConnectionInfo.class.getClassLoader().getResource("properties.default.xml");
        if (settingsURL != null) {
            File settings = new File(settingsURL.getFile());
            try {
                try (FileInputStream input = new FileInputStream(settings)) {
                    properties.loadFromXML(input);
                    String server = properties.getProperty("server");
                    int port = Integer.parseInt(properties.getProperty("port"));
                    String database = properties.getProperty("database");
                    ci.user = properties.getProperty("user");
                    ci.pass = properties.getProperty("pass");
                    ci.connectionString = "jdbc:postgresql://" + server + ":" + Integer.toString(port) + "/" + database;
                    return ci;
                }
            } catch (Exception e) {
                throw new ConnectionException(e);
            }
        } else throw new ConnectionException("Neither default nor custom properties file found.");
    }

    public static Connection getPostgresConnection() throws ConnectionException {
        return getConnection("org.postgresql.Driver", getPostgresConnectionInfo());
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
