package org.mp.naumann.algorithms;

import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.result.ResultListener;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.handler.BatchHandler;

import java.util.Collection;

/**
 * Offers an interface to implement an incremental algorithm
 *
 * An incremental algorithm works, in contrast to {@link InitialAlgorithm}, only on batches.
 * It is initialized with the intermediate datastructure of the initial algorithm and then is called
 * every time again for an incoming batch.
 *
 * @param <T> The type of the result of this algorithm
 * @param <R> The intermediate datastructure used for linking the initial with the incremental
 *            algorithm
 */
public interface IncrementalAlgorithm<T, R> extends BatchHandler {

    /**
     * This method is called by the batch processor and should take care of forwarding the batch to
     * the actual execute method.
     *
     * @param batch The batch that this algorithm should handle.
     */
    @Override
    default void handleBatch(Batch batch) {
        T result = null;
        try {
            result = execute(batch);
        } catch (AlgorithmExecutionException e) {
            e.printStackTrace();
        }
        for (ResultListener<T> resultListener : getResultListeners()) {
            resultListener.receiveResult(result);
        }
    }

    /**
     * Returns the collection of result listeners associated with this incremental algorithm
     *
     * @return The collection of result listeners
     */
    Collection<ResultListener<T>> getResultListeners();

    /**
     * Adds a result listener to this incremental algorithm.
     * The results of the execution are afterwards passed to the result listener
     *
     * @param listener The result listener to add
     */
    void addResultListener(ResultListener<T> listener);

    /**
     * Executes the algorithm for the current batch.
     *
     * @param batch The batch to execute the algorithm with
     * @return The result of the execution
     * @throws AlgorithmExecutionException Is thrown if an error occurs during executing
     */
    T execute(Batch batch) throws AlgorithmExecutionException;

    /**
     * Intializes the incremental algorithm with an intermeidate datastructure. This datastructure
     * ideally comes from the matching initial algorithm.
     *
     * @param intermediateDataStructure The intermediate datastructure coming from the initial
     *                                  algorithm
     */
    void initialize(R intermediateDataStructure);
}
