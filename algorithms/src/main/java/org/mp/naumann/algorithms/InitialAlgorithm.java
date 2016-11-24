package org.mp.naumann.algorithms;

public interface InitialAlgorithm<T, R> {

	public R getIntermediateDataStructure();

	public T execute();
}
