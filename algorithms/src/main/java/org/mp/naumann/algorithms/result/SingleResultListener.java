package org.mp.naumann.algorithms.result;

/**
 * Receives a single result and stores it. The SingleResultListener is not able to store more than
 * one Results.
 *
 * @param <T> The type of the result
 */
public class SingleResultListener<T> implements ResultListener<T> {

    private T result;

    @Override
    public void receiveResult(T result) {
        this.result = result;
    }

    public T getResult() {
        return result;
    }

}
