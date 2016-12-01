package org.mp.naumann;


import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.benchmarks.AverageAlgorithmBenchmark;
import org.mp.naumann.benchmarks.IncrementalFDBenchmark;
import org.mp.naumann.database.ConnectionException;

import java.util.logging.Level;

public class BenchmarksApplication {

    private static final int BATCH_SIZE = 100;

	public static void main(String[] args) {
        FDLogger.setLevel(Level.OFF);
        setUp();

        try {
            runIncrementalFDBenchmarks();
        } catch (ConnectionException e) {
            SpeedBenchmark.end(BenchmarkLevel.BENCHMARK, "Benchmark crashed");
            SpeedBenchmark.disable();
        }finally {
            tearDown();
        }
	}

	public static void runAverageAlgorithmBenchmarks() throws ConnectionException {
        AverageAlgorithmBenchmark benchmark = new AverageAlgorithmBenchmark();

        benchmark.constructTestCase("Adults file", "inserts.adult.csv", BATCH_SIZE, "c1", "adult");
        benchmark.runInitial();
        benchmark.runIncremental();
    }

    public static void runIncrementalFDBenchmarks() throws ConnectionException {
        IncrementalFDBenchmark benchmark = new IncrementalFDBenchmark();

        benchmark.constructInitialOnly("Adults file 15k - initial only", "benchmark", "adult");
        benchmark.runInitial();

        benchmark.constructInitialOnly("Adults file 15k + 100 - initial only", "benchmark", "adult15a1");
        benchmark.runInitial();

        benchmark.constructTestCase("Adults file", "inserts.adult.csv", "benchmark", "adult", BATCH_SIZE, -1);
        benchmark.runInitial();
        benchmark.runIncremental();




    }

	public static void setUp(){
        SpeedBenchmark.enable();
        SpeedBenchmark.addEventListener(e -> {
            if(e.getLevel() == BenchmarkLevel.ALGORITHM
                    || e.getLevel() == BenchmarkLevel.BATCH)
                System.out.println(e);
        });
        SpeedBenchmark.begin(BenchmarkLevel.BENCHMARK);
    }

    public static void tearDown(){
        SpeedBenchmark.end(BenchmarkLevel.BENCHMARK, "Finished complete benchmark");
        SpeedBenchmark.disable();
    }


}
