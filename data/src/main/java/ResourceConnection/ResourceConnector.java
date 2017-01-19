package ResourceConnection;

import java.io.InputStream;
import java.net.URL;

public class ResourceConnector {

    public static final String TEST = "test/";
    public static final String BASELINE = "baseline/";
    public static final String UPDATE = "update/";
    public static final String BENCHMARK = "benchmark/";
    public static final String FULL_BATCHES = "full_batches/";

    public static InputStream getResource(String folder, String filename) {
        return ResourceConnector.class.getResourceAsStream(folder + "/" + filename);
    }

    private static String getResourceIfExists(URL url) {
        return (url == null) ? "" : url.getPath();
    }

    public static String getResourcePath(String folder, String filename) {
        return getResourcePath(folder + "/" + filename);
    }

    public static String getResourcePath(String filename) {
        URL url = ResourceConnector.class.getResource(filename);
        return getResourceIfExists(url);
    }

}
