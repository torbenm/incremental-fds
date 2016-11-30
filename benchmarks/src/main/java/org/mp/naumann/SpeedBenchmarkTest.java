package org.mp.naumann;

import org.mp.naumann.algorithms.IncrementalAlgorithm;
import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.processor.BatchProcessor;
import org.mp.naumann.processor.SynchronousBatchProcessor;
import org.mp.naumann.processor.batch.source.BatchSource;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;
import org.mp.naumann.processor.handler.database.DatabaseBatchHandler;

public class SpeedBenchmarkTest<T, R> {

    private BatchProcessor batchProcessor;
    private StreamableBatchSource batchSource;
    private DatabaseBatchHandler databaseBatchHandler;

    private final InitialAlgorithm<T, R> initialAlgorithm;
    private final IncrementalAlgorithm<T, R> incrementalAlgorithm;
    private SpeedBenchmark speed;

    private String currentTestName = "No test defined";

    public SpeedBenchmarkTest(InitialAlgorithm<T, R> initialAlgorithm, IncrementalAlgorithm<T, R> incrementalAlgorithm){
        this.initialAlgorithm = initialAlgorithm;
        this.incrementalAlgorithm = incrementalAlgorithm;

    }

    public BatchProcessor getBatchProcessor() {
        return batchProcessor;
    }

    public BatchSource getBatchSource() {
        return batchSource;
    }

    public DatabaseBatchHandler getDatabaseBatchHandler() {
        return databaseBatchHandler;
    }

    public InitialAlgorithm<T, R> getInitialAlgorithm() {
        return initialAlgorithm;
    }

    public IncrementalAlgorithm<T, R> getIncrementalAlgorithm() {
        return incrementalAlgorithm;
    }

    public void setDataSources(StreamableBatchSource batchSource, DatabaseBatchHandler databaseBatchHandler){
        this.batchSource = batchSource;
        this.databaseBatchHandler = databaseBatchHandler;
        this.batchProcessor = new SynchronousBatchProcessor(batchSource, databaseBatchHandler);
        this.batchProcessor.addBatchHandler(incrementalAlgorithm);
    }

    public void runInitial(String testName){
        this.currentTestName = testName;
        SpeedBenchmark.begin(BenchmarkLevel.ALGORITHM);
        initialAlgorithm.execute();
        SpeedBenchmark.end(BenchmarkLevel.ALGORITHM, "Finished initial Algorithm");
    }

    public void runIncremental(String testName){
        this.currentTestName = testName;
        SpeedBenchmark.begin(BenchmarkLevel.ALGORITHM);
        batchSource.startStreaming();
        SpeedBenchmark.end(BenchmarkLevel.ALGORITHM, "Finished Incremental Algorithm Execution");
    }
}
