package org.mp.naumann.algorithms.result;

import java.io.PrintStream;

public class PrintResultListener<T> implements ResultListener<T> {

	private final PrintStream out;
	private final String name;

	public PrintResultListener(String name) {
		this(name, System.out);
	}

	public PrintResultListener(String name, PrintStream out) {
		this.name = name;
		this.out = out;
	}

	@Override
	public void receiveResult(T result) {
		out.println(name + ": " + result);
	}

}
