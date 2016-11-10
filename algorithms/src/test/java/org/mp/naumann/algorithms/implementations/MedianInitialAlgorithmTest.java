package org.mp.naumann.algorithms.implementations;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.jdbc.JdbcDataConnector;
import org.mp.naumann.database.utils.PostgresConnection;

import java.io.IOException;

import static junit.framework.TestCase.assertEquals;


public class MedianInitialAlgorithmTest {

    private static DataConnector dataConnector;
    private static MedianInitialAlgorithm algorithm;

    private static final String schema = "test";
    private static final String tableName = "countries";
    private static final String columnName = "population";

    @BeforeClass
    public static void setUp() throws IOException {
        dataConnector = new JdbcDataConnector("org.postgresql.Driver", PostgresConnection.getConnectionInfo());
        algorithm = new MedianInitialAlgorithm(dataConnector, schema, tableName, columnName);
    }

    @Test
    public void testExecute(){
        String result = algorithm.execute();
        assertEquals("3286936", result);
    }

}
