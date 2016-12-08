package org.mp.naumann.database.utils;

import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.jdbc.ConnectionInfo;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class ConnectionManager {

    private static final String defaultCsvSeparator = ",";
    private static final String defaultCsvDir = "";

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
                    int port = Integer.parseInt(properties.getProperty("port"));
                    String database = properties.getProperty("database");
                    ci.user = properties.getProperty("user");
                    ci.pass = properties.getProperty("pass");
                    ci.connectionString = "jdbc:postgresql://localhost:" + Integer.toString(port) + "/" + database;
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

    public static Connection getCsvConnection(String csvDir, String separator) throws ConnectionException {
        return getCsvConnection(ConnectionManager.class, csvDir, separator);
    }

    public static Connection getCsvConnection(Class<?> clazz, String csvDir, String separator) throws ConnectionException {
        URL csvResourceURL = clazz.getClassLoader().getResource("csv");
        if (csvResourceURL == null)
            throw new ConnectionException("Can't find resource directory");
        ConnectionInfo ci = new ConnectionInfo();
        ci.connectionString = "jdbc:relique:csv:" + csvResourceURL.getFile() + csvDir + "?separator=" + separator;
        return getConnection("org.relique.jdbc.csv.CsvDriver", ci);
    }

    public static Connection getCsvConnection(String csvDir) throws ConnectionException {
        return getCsvConnection(csvDir, defaultCsvSeparator);
    }

    public static Connection getCsvConnection() throws ConnectionException {
        return getCsvConnection(defaultCsvDir, defaultCsvSeparator);
    }

}
