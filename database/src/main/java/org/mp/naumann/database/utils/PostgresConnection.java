package org.mp.naumann.database.utils;

import org.mp.naumann.database.jdbc.ConnectionInfo;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class PostgresConnection {

    public static ConnectionInfo getConnectionInfo() {
        ConnectionInfo ci = new ConnectionInfo();
        Properties properties = new Properties();
        File settings = new File("../properties.xml");
        if (!settings.exists()) settings = new File("../properties.default.xml");
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
        return null;
    }

}
