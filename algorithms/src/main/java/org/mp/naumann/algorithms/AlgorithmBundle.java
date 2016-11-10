package org.mp.naumann.algorithms;

import org.mp.naumann.processor.BatchProcessor;

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

    public void executeInitialAlgorithm(){
        initialAlgorithm.execute();
        incrementalAlgorithm.setIntermediateDataStructure(initialAlgorithm.getIntermediateDataStructure());
    }

    public void attachToBatchProcessor(BatchProcessor batchProcessor){
        batchProcessor.addBatchHandler(incrementalAlgorithm);
    }
}
