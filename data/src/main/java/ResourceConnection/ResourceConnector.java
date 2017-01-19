package ResourceConnection;

import java.io.InputStream;

public class ResourceConnector {

    public static InputStream getResource(ResourceType type, String filename) {
        return ResourceConnector.class.getResourceAsStream(type.getPath() + filename);
    }

    public static String getResourcePath(ResourceType type, String filename) {
        return ResourceConnector.class.getResource(type.getPath() + filename).getPath();
    }

}
