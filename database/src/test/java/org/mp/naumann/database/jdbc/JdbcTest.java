package org.mp.naumann.database.jdbc;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.utils.PostgresConnection;

@SuppressWarnings("WeakerAccess")
public class JdbcTest {

    protected static final String schema = "test";
    protected static final String tableName = "countries";
    static DataConnector connector;

    @BeforeClass
    public static void setUpOnce() throws Exception {
        connector = new JdbcDataConnector("org.postgresql.Driver", PostgresConnection.getConnectionInfo());
    }

    @AfterClass
    public static void tearDownOnce() {
        connector.disconnect();
    }

}
