package org.mp.naumann;

import org.mp.naumann.algorithms.IncrementalAlgorithm;
import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.implementations.AverageDatastructure;
import org.mp.naumann.algorithms.implementations.AverageIncrementalAlgorithm;
import org.mp.naumann.processor.batch.source.CsvFileBatchSource;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.fake.FakeDatabaseBatchHandler;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;

public class BenchmarksApplication {

    private static final String TABLE = "test.countries";
    private static final int BATCH_SIZE = 10;
    private static final String SCHEMA = "test";

	public static void main(String[] args) {

        SpeedBenchmark.enable();
        SpeedBenchmark.addEventListener(System.out::println);
        SpeedBenchmark.begin(BenchmarkLevel.BENCHMARK);

        String file = BenchmarksApplication.class.getResource("countries.csv").getPath();
        StreamableBatchSource batchSource = new CsvFileBatchSource(file, SCHEMA, TABLE, BATCH_SIZE);
        DatabaseBatchHandler databaseBatchHandler = new FakeDatabaseBatchHandler();

        AverageDatastructure popDs = new AverageDatastructure();

        IncrementalAlgorithm<Double, AverageDatastructure> popAvg = new AverageIncrementalAlgorithm("population");
        popAvg.setIntermediateDataStructure(popDs);
        SpeedBenchmarkTest speedBenchmarkTest = new SpeedBenchmarkTest<>(null, popAvg);

        speedBenchmarkTest.setDataSources(batchSource, databaseBatchHandler);

        speedBenchmarkTest.runIncremental("Countries Test File");

        SpeedBenchmark.end(BenchmarkLevel.BENCHMARK, "Finished complete benchmark.");
        SpeedBenchmark.disable();

	}
}
