package org.mp.naumann.algorithms.fd.hyfd;

import org.mp.naumann.algorithms.fd.FDAlgorithmTest;
import org.mp.naumann.algorithms.fd.FunctionalDependencyAlgorithm;

public class HyFDTest extends FDAlgorithmTest {


	@Override
	protected FunctionalDependencyAlgorithm getNewInstance() {
		return new HyFD();
	}
}
