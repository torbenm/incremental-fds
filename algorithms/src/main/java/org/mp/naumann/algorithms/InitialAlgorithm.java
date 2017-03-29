package org.mp.naumann.algorithms;

/**
 * Offers an interface to implement an initial algorithm.
 *
 * An initial algorithm is an algorithm that is executed exactly once at the time when the system is
 * setup. It <b>only</b> works on the pre-existing dataset and is <u>not</u> executed again for or
 * before every batch.
 *
 * During its course, the initial algorithm is supposed to create an intermediate datastructure,
 * which can be reused by the {@link InitialAlgorithm}. This datastructure stores any additional
 * information that is necessary for further processing, but information that is not necessarily
 * actually wanted output of the algorithm.
 *
 * Also, it creates a final result, which is returned by the execute method.
 *
 * @param <T> The type of the result of this algorithm
 * @param <R> The type of the intermediate datastructure.
 */
public interface InitialAlgorithm<T, R> {

    /**
     * Returns the intermediate data structure.
     * It should only be available after {@link #execute()} had been called.
     *
     * @return The intermediate data structure
     */
    R getIntermediateDataStructure();

    /**
     * Executes the initial algorithm.
     *
     * @return Returns the results of the initial algorithm
     */
    T execute();
}
