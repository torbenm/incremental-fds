package org.mp.naumann;


import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDVersion;
import org.mp.naumann.benchmarks.IncrementalFDBenchmark;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.reporter.GoogleSheetsReporter;
import org.mp.naumann.testcases.InitialAndIncrementalOneBatch;
import org.mp.naumann.testcases.TestCase;

import java.io.IOException;
import java.util.logging.Level;

public class BenchmarksApplication {

    private static GoogleSheetsReporter googleSheetsReporter;

    static {
        try {
            googleSheetsReporter = new GoogleSheetsReporter();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        int version = args.length >= 1 ? Integer.valueOf(args[0]) : 1;
        int batchSize = args.length >= 2 ? Integer.valueOf(args[1]) : 100;
        long splitLine = args.length >= 3 ? Long.valueOf(args[2]) : 15000;
        String dataSet = args.length >= 4 ? args[3] : "benchmark.adultfull.csv";

        int stopAfter = batchSize < 10 ? 100 : -1;

        FDLogger.setLevel(Level.OFF);
        setUp();

        try {
            TestCase t = new InitialAndIncrementalOneBatch(splitLine,
                    batchSize,
                    dataSet,
                    new IncrementalFDBenchmark(IncrementalFDVersion.valueOf(version)),
                    stopAfter
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
