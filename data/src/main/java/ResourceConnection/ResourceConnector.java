package ResourceConnection;

import java.io.InputStream;

/**
 * Created by dennis on 01.12.16.
 */
public class ResourceConnector {

    private static ResourceConnector instance = null;

    private ResourceConnector() {    }

    public static ResourceConnector getInstance() {
        if (instance == null) {
            instance = new ResourceConnector();
        }
        return instance;
    }

    public static InputStream getResource(ResourceType type, String filename) {
        return ResourceConnector.class.getResourceAsStream(type.getPath() + filename);
    }

    public static String getResourcePath(ResourceType type, String filename) {
        return ResourceConnector.class.getResource(type.getPath() + filename).getPath();
    }
}
