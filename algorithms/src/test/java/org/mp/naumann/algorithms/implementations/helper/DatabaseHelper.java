package org.mp.naumann.algorithms.implementations.helper;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.mp.naumann.database.jdbc.JdbcDataConnector;
import org.mp.naumann.database.DataConnector;

public class DatabaseHelper {

    private static final String originalPath = "src/test/data/csv";
    private static final File originalDir = new File(originalPath);
    private static final String testPath = "src/test/data/csv_test";
    private static final File testDir = new File(testPath);

    public static void prepareDataset() throws IOException {
        FileUtils.deleteQuietly(testDir);
        FileUtils.copyDirectory(originalDir, testDir);
    }

    public static DataConnector getDataConnector(){
        return new JdbcDataConnector("org.relique.jdbc.csv.CsvDriver", "jdbc:relique:csv:" + testPath);
    }

}

