package org.mp.naumann.algorithms;

import java.io.PrintStream;

public class PrintResultReceiver implements FunctionalDependencyResultReceiver {

	private final PrintStream out;

	public PrintResultReceiver() {
		this.out = System.out;
	}

	public PrintResultReceiver(PrintStream out) {
		this.out = out;
	}

	@Override
	public void receiveResult(FunctionalDependency functionalDependency) {
		out.println(functionalDependency);
	}

}
