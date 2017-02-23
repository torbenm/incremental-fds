package org.mp.naumann.algorithms.implementations;

import com.opentable.db.postgres.embedded.DatabasePreparer;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.jdbc.JdbcDataConnector;
import org.mp.naumann.database.utils.PostgresConnectionPreparer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class MedianInitialAlgorithmTest {

    private static DataConnector dataConnector;
    private static MedianInitialAlgorithm algorithm;

    private static final String schema = "test";
    private static final String tableName = "countries";
    private static final String columnName = "population";
    private static final DatabasePreparer preparer = new PostgresConnectionPreparer();

    @ClassRule
    static public PreparedDbRule pr = EmbeddedPostgresRules.preparedDatabase(preparer);


    @BeforeClass
    public static void setUp() throws IOException, SQLException {
        Connection conn = pr.getTestDatabase().getConnection();
        dataConnector = new JdbcDataConnector(conn);
        algorithm = new MedianInitialAlgorithm(dataConnector, schema, tableName, columnName);
    }

    @Test
    public void testExecute(){
        String result = algorithm.execute();
        assertEquals("3286936", result);
    }

    @AfterClass
    public static void tearDownOnce() throws ConnectionException {
        dataConnector.close();
    }

}
