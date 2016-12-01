package org.mp.naumann.benchmarks;

import org.mp.naumann.algorithms.IncrementalAlgorithm;
import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;

public interface AlgorithmBenchmark<T, R> {

    default T runInitial() {
        SpeedBenchmark.begin(BenchmarkLevel.ALGORITHM);
        T res = getInitialAlgorithm().execute();
        SpeedBenchmark.end(BenchmarkLevel.ALGORITHM, "Finished initial algorithm for test case "+getCurrentTestCase());
        return res;
    }



    default void runIncremental(){
        SpeedBenchmark.begin(BenchmarkLevel.ALGORITHM);
        getBatchSource().startStreaming();
        SpeedBenchmark.end(BenchmarkLevel.ALGORITHM, "Finished incremental algorithm for test case "+getCurrentTestCase());
    }

    default void runBoth(){
        runInitial();
        runIncremental();
    }

    String getCurrentTestCase();
    StreamableBatchSource getBatchSource();
    InitialAlgorithm<T,R> getInitialAlgorithm();
    IncrementalAlgorithm<T, R> getIncrementalAlgorithm();
}
