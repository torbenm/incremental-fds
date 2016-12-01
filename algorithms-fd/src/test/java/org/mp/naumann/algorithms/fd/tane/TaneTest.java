package org.mp.naumann.algorithms.fd.tane;

import org.mp.naumann.algorithms.fd.FDAlgorithmTest;
import org.mp.naumann.algorithms.fd.FunctionalDependencyAlgorithm;

public class TaneTest extends FDAlgorithmTest{
    @Override
    protected FunctionalDependencyAlgorithm getNewInstance() {
        return new TaneAlgorithm();
    }
}
