package org.mp.naumann.benchmarks;

import org.mp.naumann.algorithms.IncrementalAlgorithm;
import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.processor.batch.source.StreamableBatchSource;

public interface AlgorithmBenchmark<T, R> {

    default T runInitial() {
        T res = getInitialAlgorithm().execute();
        return res;
    }


    default void runIncremental() {
        getBatchSource().startStreaming();
    }

    default void runBoth() {
        runInitial();
        runIncremental();
    }

    String getCurrentTestCase();

    StreamableBatchSource getBatchSource();

    InitialAlgorithm<T, R> getInitialAlgorithm();

    IncrementalAlgorithm<T, R> getIncrementalAlgorithm();
}
