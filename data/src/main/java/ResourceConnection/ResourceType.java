package ResourceConnection;

/**
 * Created by dennis on 01.12.16.
 */
public enum ResourceType {
    TEST("test/"),
    BASELINE("baseline/"),
    UPDATE("update/"),
    BENCHMARK("benchmark/");

    private String path;

    ResourceType(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }
}
