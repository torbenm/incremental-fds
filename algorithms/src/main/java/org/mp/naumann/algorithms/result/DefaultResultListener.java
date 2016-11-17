package org.mp.naumann.algorithms.result;


public class DefaultResultListener<T> implements ResultListener<T> {

	private T result;

	@Override
	public void receiveResult(T result) {
		this.result = result;
	}

	public T getResult() {
		return result;
	}
	
}
