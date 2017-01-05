package org.mp.naumann.algorithms;

import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.result.ResultListener;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.handler.BatchHandler;

import java.util.Collection;

public interface IncrementalAlgorithm<T, R> extends BatchHandler {

	@Override
	default void handleBatch(Batch batch) {
		T result = null;
		try {
			result = execute(batch);
		} catch (AlgorithmExecutionException e) {
			//TODO
			e.printStackTrace();
		}
		for (ResultListener<T> resultListener : getResultListeners()) {
			resultListener.receiveResult(result);
		}
	}

	Collection<ResultListener<T>> getResultListeners();

	void addResultListener(ResultListener<T> listener);

    void initialize();

    T execute(Batch batch) throws AlgorithmExecutionException;

	void setIntermediateDataStructure(R intermediateDataStructure);
}
