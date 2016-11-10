package org.mp.naumann.database.jdbc;

import com.opentable.db.postgres.embedded.DatabasePreparer;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.utils.PostgresConnectionPreparer;

import java.sql.Connection;

@SuppressWarnings("WeakerAccess")
public class JdbcTest {

    static final String schema = "test";
    static final String tableName = "countries";
    static DataConnector connector;
    private static final DatabasePreparer preparer = new PostgresConnectionPreparer();

    @ClassRule
    static public PreparedDbRule pr = EmbeddedPostgresRules.preparedDatabase(preparer);

    @BeforeClass
    public static void setUpOnce() throws Exception {
        Connection conn = pr.getTestDatabase().getConnection();
        connector = new JdbcDataConnector(conn);
    }

    @AfterClass
    public static void tearDownOnce() {
        connector.disconnect();
    }

}
