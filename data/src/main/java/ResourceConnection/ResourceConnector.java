package ResourceConnection;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class ResourceConnector {

    public static final String TEST = "../data_files/test/";
    public static final String BASELINE = "../data_files/baseline/";
    public static final String UPDATE = "../data_files/update/";
    public static final String BENCHMARK = "../data_files/benchmark/";
    public static final String FULL_BATCHES = "../data_files/full_batches/";

    public static InputStream getResource(String folder, String filename) {
        try {
            return new FileInputStream(getResourcePath(folder, filename));
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    public static String getResourcePath(String folder, String filename) {
        return getResourcePath((folder + "/" + filename).replace("//", "/"));
    }

    public static String getResourcePath(String filename) {
        return filename;
    }

}
