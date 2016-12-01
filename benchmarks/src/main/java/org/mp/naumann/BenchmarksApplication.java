package org.mp.naumann;


import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.benchmarks.AverageAlgorithmBenchmark;
import org.mp.naumann.database.ConnectionException;

public class BenchmarksApplication {

    private static final int BATCH_SIZE = 100;

	public static void main(String[] args) {

        setUp();

        try {
            runAverageAlgorithmBenchmarks();
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

	public static void setUp(){
        SpeedBenchmark.enable();
        SpeedBenchmark.addEventListener(System.out::println);
        SpeedBenchmark.begin(BenchmarkLevel.BENCHMARK);
    }

    public static void tearDown(){
        SpeedBenchmark.end(BenchmarkLevel.BENCHMARK, "Finished complete benchmark");
        SpeedBenchmark.disable();
    }


}
