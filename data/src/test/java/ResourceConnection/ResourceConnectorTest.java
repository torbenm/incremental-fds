package ResourceConnection;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.*;

/**
 * Created by dennis on 01.12.16.
 */
public class ResourceConnectorTest {

    private static ResourceConnector rc;

    @BeforeClass
    public static void setUp() throws IOException {
        rc = ResourceConnector.getInstance();
    }

    @Test
    public void getsCSVWithCorrectType() {
        InputStream testInputStream = rc.getResource(ResourceType.TEST, "test.unit.csv");
        assertNotNull("InputStream should not be null", testInputStream);
    }

    @Test
    public void returnsNullWithWrongType() {
        InputStream testInputStream = rc.getResource(ResourceType.BASELINE, "test.unit.csv");
        assertNull("InputStream should be null", testInputStream);
    }

    @Test
    public void getsPathOfCSVWithCorrectType() {
        String path = rc.getResourcePath(ResourceType.TEST, "test.unit.csv");
        assertEquals("Path should be equal", System.getProperty("user.dir") + "/target/test-classes/ResourceConnection/test/test.unit.csv", path);
    }



}