package org.mp.naumann;


import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.benchmarks.AverageAlgorithmBenchmark;
import org.mp.naumann.benchmarks.IncrementalFDBenchmark;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.reporter.GoogleSheetsReporter;
import org.mp.naumann.testcases.InitialAndIncrementalOneBatch;
import org.mp.naumann.testcases.TestCase;

import java.io.IOException;
import java.util.logging.Level;

public class BenchmarksApplication {

    private static final int BATCH_SIZE = 100;
    private static GoogleSheetsReporter googleSheetsReporter;

    static {
        try {
            googleSheetsReporter = new GoogleSheetsReporter();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        FDLogger.setLevel(Level.OFF);
        setUp();

        try {
            TestCase t = new InitialAndIncrementalOneBatch(15000,
                    BATCH_SIZE,
                    "benchmark.adultfull.csv",
                    new IncrementalFDBenchmark()
            );
            t.execute();
            googleSheetsReporter.writeNewLine(
                    t.sheetName(),
                    t.sheetValues()
            );

        } catch (ConnectionException e) {
            SpeedBenchmark.end(BenchmarkLevel.BENCHMARK, "Benchmark crashed");
            SpeedBenchmark.disable();
        } catch (IOException e) {
            SpeedBenchmark.end(BenchmarkLevel.BENCHMARK, "Writing to GoogleSheets crashed");
            SpeedBenchmark.disable();
        } finally {
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
