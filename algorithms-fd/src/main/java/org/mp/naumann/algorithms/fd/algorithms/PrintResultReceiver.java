package org.mp.naumann.algorithms.fd.algorithms;

import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.FunctionalDependencyResultReceiver;

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
