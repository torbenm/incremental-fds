package org.mp.naumann.data;

import org.junit.Test;

import java.io.File;
import java.io.InputStream;

import static org.junit.Assert.*;

public class ResourceConnectorTest {

    private static String testFile = "test.csv";

    @Test
    public void getsCSVWithCorrectType() {
        InputStream testInputStream = ResourceConnector.getResource(ResourceConnector.TEST, testFile);
        assertNotNull("InputStream should not be null", testInputStream);
    }

    @Test
    public void returnsNullWithWrongType() {
        InputStream testInputStream = ResourceConnector.getResource(ResourceConnector.BASELINE, testFile);
        assertNull("InputStream should be null", testInputStream);
    }

    @Test
    public void getsPathOfCSVWithCorrectType() {
        File actual = new File(ResourceConnector.getResourcePath(ResourceConnector.TEST, testFile));
        File expected = new File("../data_files/test/" + testFile);
        assertEquals("Path should be equal", expected.getPath(), actual.getPath());
    }

}