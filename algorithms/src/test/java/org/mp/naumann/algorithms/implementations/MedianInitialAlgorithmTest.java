package org.mp.naumann.algorithms.implementations;

import static junit.framework.TestCase.assertEquals;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mp.naumann.algorithms.implementations.helper.DatabaseHelper;
import org.mp.naumann.database.DataConnector;


public class MedianInitialAlgorithmTest {

    private static DataConnector dataConnector;
    private static MedianInitialAlgorithm algorithm;

    protected static final String tableName = "median";
    protected static final String columnName = "age";
    private static final String schema = "";

    @BeforeClass
    public static void setUp() throws IOException {
        DatabaseHelper.prepareDataset();
        dataConnector = DatabaseHelper.getDataConnector();
        algorithm = new MedianInitialAlgorithm(dataConnector, schema, tableName, columnName);
    }

    @Test
    public void testExecute(){
        // As CSV files are loaded as String tables, this method
        // sorts the numbers alphabetically - not numberwise.
        String result = algorithm.execute();
        assertEquals("19", result);
    }



}
