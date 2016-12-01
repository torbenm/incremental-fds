package org.mp.naumann.algorithms;

import java.util.Collection;
import org.mp.naumann.algorithms.result.ResultListener;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.handler.BatchHandler;

public interface IncrementalAlgorithm<T, R> extends BatchHandler {

	@Override
	default void handleBatch(Batch batch) {
		T result = execute(batch);
		for (ResultListener<T> resultListener : getResultListeners()) {
			resultListener.receiveResult(result);
		}
	}

	Collection<ResultListener<T>> getResultListeners();

	void addResultListener(ResultListener<T> listener);

    void initialize();

    T execute(Batch batch);

	void setIntermediateDataStructure(R intermediateDataStructure);
}
