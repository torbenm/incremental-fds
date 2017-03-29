package org.mp.naumann.algorithms.result;


/**
 * Interface for classes that want to receive results from an {@link
 * org.mp.naumann.algorithms.IncrementalAlgorithm} or {@link org.mp.naumann.algorithms.InitialAlgorithm}
 *
 * @param <T> The type of the result
 */
public interface ResultListener<T> {

    /**
     * Is called when a result is ready to be received
     *
     * @param result The result itself
     */
    void receiveResult(T result);
}
