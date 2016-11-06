package org.mp.naumann.algorithms;

import org.mp.naumann.processor.BatchProcessor;

public abstract class AlgorithmBundle {

    private final InitialAlgorithm initialAlgorithm;
    private final IncrementalAlgorithm incrementalAlgorithm;

    public AlgorithmBundle(InitialAlgorithm initialAlgorithm, IncrementalAlgorithm incrementalAlgorithm) {
        this.initialAlgorithm = initialAlgorithm;
        this.incrementalAlgorithm = incrementalAlgorithm;

        incrementalAlgorithm.setIntermediateDataStructure(initialAlgorithm.getIntermediateDataStructure());

    }

    public InitialAlgorithm getInitialAlgorithm() {
        return initialAlgorithm;
    }

    public IncrementalAlgorithm getIncrementalAlgorithm() {
        return incrementalAlgorithm;
    }

    public void executeInitialAlgorithm(){
        initialAlgorithm.execute();
    }

    public void attachToBatchProcessor(BatchProcessor batchProcessor){
        batchProcessor.addBatchHandler(incrementalAlgorithm);
    }
}
