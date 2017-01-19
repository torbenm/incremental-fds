package ResourceConnection;

public enum ResourceType {
    TEST("test/"),
    BASELINE("baseline/"),
    UPDATE("update/"),
    BENCHMARK("benchmark/"),
    FULL_BATCHES("full_batches/");

    private String path;

    ResourceType(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }
}
