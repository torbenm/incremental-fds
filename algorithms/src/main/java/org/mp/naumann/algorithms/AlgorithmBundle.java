package org.mp.naumann.algorithms;

import org.mp.naumann.processor.BatchProcessor;

/**
 * AlgorithmBundles represent a combination of an {@link InitialAlgorithm} and an {@link
 * IncrementalAlgorithm}. THe Intermediate Datastructure is automatically exchanged between the two
 * of them.
 *
 * @param <T> The Type of the Intermediate Datastructure
 * @param <R> The Type of the Algorithm Result, which will be sent to a {@link
 *            org.mp.naumann.algorithms.result.ResultListener}
 */
public abstract class AlgorithmBundle<T, R> {

    private final InitialAlgorithm<T, R> initialAlgorithm;
    private final IncrementalAlgorithm<T, R> incrementalAlgorithm;

    public AlgorithmBundle(InitialAlgorithm<T, R> initialAlgorithm, IncrementalAlgorithm<T, R> incrementalAlgorithm) {
        this.initialAlgorithm = initialAlgorithm;
        this.incrementalAlgorithm = incrementalAlgorithm;

    }

    public InitialAlgorithm<T, R> getInitialAlgorithm() {
        return initialAlgorithm;
    }

    public IncrementalAlgorithm<T, R> getIncrementalAlgorithm() {
        return incrementalAlgorithm;
    }

    public void executeInitialAlgorithm() {
        initialAlgorithm.execute();
        incrementalAlgorithm.initialize(initialAlgorithm.getIntermediateDataStructure());
    }

    public void attachToBatchProcessor(BatchProcessor batchProcessor) {
        batchProcessor.addBatchHandler(incrementalAlgorithm);
    }
}
