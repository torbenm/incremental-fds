package ResourceConnection;

import org.junit.Test;

import java.io.File;
import java.io.InputStream;

import static org.junit.Assert.*;

public class ResourceConnectorTest {

    @Test
    public void getsCSVWithCorrectType() {
        InputStream testInputStream = ResourceConnector.getResource(ResourceType.TEST, "test.unit.csv");
        assertNotNull("InputStream should not be null", testInputStream);
    }

    @Test
    public void returnsNullWithWrongType() {
        InputStream testInputStream = ResourceConnector.getResource(ResourceType.BASELINE, "test.unit.csv");
        assertNull("InputStream should be null", testInputStream);
    }

    @Test
    public void getsPathOfCSVWithCorrectType() {
        File actual = new File(ResourceConnector.getResourcePath(ResourceType.TEST, "test.unit.csv"));
        File expected = new File(System.getProperty("user.dir") + "/target/test-classes/ResourceConnection/test/test.unit.csv");
        assertEquals("Path should be equal", expected.getPath(), actual.getPath());
    }

}