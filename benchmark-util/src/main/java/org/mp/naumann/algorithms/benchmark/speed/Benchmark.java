package org.mp.naumann.algorithms.benchmark.speed;

import java.util.HashSet;
import java.util.Set;

/**
 * Offers methods for time benchmarking.
 */
public class Benchmark {

    public static final int DEFAULT_LEVEL = 0;
    private static final Set<BenchmarkEventListener> eventListeners = new HashSet<>();
    private static int maxLevel;
    private final long startTime;
    private final int level;
    private final String description;
    private long startLapTime;

    private Benchmark(String description, int level) {
        this.level = level;
        this.description = description;
        this.startTime = System.nanoTime();
        this.startLapTime = startTime;
    }

    public static void enableAll() {
        setMaxLevel(Integer.MAX_VALUE);
    }

    public static void setMaxLevel(int level) {
        maxLevel = level;
    }

    public static void disable() {
        setMaxLevel(-1);
    }

    public static Benchmark start(String description, int level) {
        return new Benchmark(description, level);
    }

    public static Benchmark start(String description) {
        return start(description, DEFAULT_LEVEL);
    }

    private static BenchmarkEvent finish(Benchmark benchmark, long duration) {
        BenchmarkEvent event = new BenchmarkEvent(duration, benchmark.getDescription(), benchmark.getLevel());
        notifyListeners(event);
        return event;
    }

    private static void notifyListeners(BenchmarkEvent event) {
        if (event.getLevel() <= maxLevel) {
            eventListeners.forEach(listener -> listener.notify(event));
        }
    }

    private static BenchmarkEvent finishSubtask(Benchmark benchmark, long duration, String subtaskDescription) {
        BenchmarkEvent event = new BenchmarkEvent(duration, benchmark.getDescription() + ": " + subtaskDescription, benchmark.getLevel() + 1);
        notifyListeners(event);
        return event;
    }

    public static void addEventListener(BenchmarkEventListener eventListener) {
        eventListeners.add(eventListener);
    }

    public BenchmarkEvent finishSubtask(String description) {
        long endLapTime = System.nanoTime();
        long duration = endLapTime - this.startLapTime;
        this.startLapTime = endLapTime;
        return finishSubtask(this, duration, description);
    }

    public BenchmarkEvent finish() {
        return finish(this, System.nanoTime() - this.startTime);
    }

    private int getLevel() {
        return level;
    }

    private String getDescription() {
        return description;
    }

    public void startSubtask() {
        this.startLapTime = System.nanoTime();
    }
}
