package org.mp.naumann.algorithms.result;

import java.io.PrintStream;

public class PrintResultReceiver<T> implements ResultListener<T> {

	private final PrintStream out;

	public PrintResultReceiver() {
		this(System.out);
	}

	public PrintResultReceiver(PrintStream out) {
		this.out = out;
	}

	@Override
	public void receiveResult(T result) {
		out.println(result);
	}

}
