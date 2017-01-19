package ResourceConnection;

import java.io.InputStream;
import java.net.URL;

public class ResourceConnector {

    public static InputStream getResource(ResourceType type, String filename) {
        return ResourceConnector.class.getResourceAsStream(type.getPath() + filename);
    }

    private static String getResourceIfExists(URL url) {
        return (url == null) ? "" : url.getPath();
    }

    public static String getResourcePath(ResourceType type, String filename) {
        URL url = ResourceConnector.class.getResource(type.getPath() + filename);
        return getResourceIfExists(url);
    }

    public static String getResourcePath(String filename) {
        URL url = ResourceConnector.class.getResource(filename);
        return getResourceIfExists(url);
    }

}
