package org.mp.naumann;

import java.io.File;

import org.mp.naumann.algorithms.AlgorithmExecutionException;
import org.mp.naumann.algorithms.CSVRelationalInputGenerator;
import org.mp.naumann.algorithms.FunctionalDependencyResultReceiver;
import org.mp.naumann.algorithms.PrintResultReceiver;
import org.mp.naumann.algorithms.RelationalInputGenerator;
import org.mp.naumann.fdep.FDEPExecutor;
import org.mp.naumann.hyfd.HyFD;
import org.mp.naumann.tane.TaneAlgorithm;

public class FDDemo {

	public static void main(String[] args) throws AlgorithmExecutionException {
		RelationalInputGenerator inputGenerator = new CSVRelationalInputGenerator(new File("data.csv"));
		FunctionalDependencyResultReceiver resultReceiver = new PrintResultReceiver();

		System.out.println("HyFD");
		HyFD hyfd = new HyFD(inputGenerator, resultReceiver);
		hyfd.execute();
		System.out.println();

		System.out.println("TANE");
		TaneAlgorithm tane = new TaneAlgorithm(inputGenerator, resultReceiver);
		tane.execute();
		System.out.println();

		System.out.println("FDEP");
		FDEPExecutor fdep = new FDEPExecutor(inputGenerator, resultReceiver);
		fdep.execute();
	}

}
