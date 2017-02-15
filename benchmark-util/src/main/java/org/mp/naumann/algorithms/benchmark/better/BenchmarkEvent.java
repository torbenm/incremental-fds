package org.mp.naumann.algorithms.benchmark.better;

public class BenchmarkEvent {

    private static final long MILLIS_IN_NANOS = 1_000_000L;
    private final long duration;
    private final String description;
    private final int level;

    BenchmarkEvent(long duration, String description, int level) {
        this.duration = duration;
        this.description = description;
        this.level = level;
    }

    public int getLevel() {
        return level;
    }

    public long getDuration() {
        return duration;
    }

    public long getDurationInMillis() {
        return duration / MILLIS_IN_NANOS;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return "Finished \"" + description + "\" in " + getDurationInMillis() + "ms";
    }
}
