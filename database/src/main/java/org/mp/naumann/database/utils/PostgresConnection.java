package org.mp.naumann.database.utils;

import org.mp.naumann.database.jdbc.ConnectionInfo;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.util.Properties;

public class PostgresConnection {

    public static ConnectionInfo getConnectionInfo() {
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
                e.printStackTrace();
            }
        }
        return null;
    }

}
