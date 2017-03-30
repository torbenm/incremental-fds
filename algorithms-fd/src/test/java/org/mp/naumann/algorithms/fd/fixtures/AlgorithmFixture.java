package org.mp.naumann.algorithms.fd.fixtures;

import org.mp.naumann.algorithms.fd.FunctionalDependencyResultReceiver;
import org.mp.naumann.database.ConnectionException;
import org.mp.naumann.database.InputReadException;
import org.mp.naumann.database.Table;

public interface AlgorithmFixture {

    Table getInputGenerator() throws ConnectionException, InputReadException;

    FunctionalDependencyResultReceiver getFunctionalDependencyResultReceiver();

    void verifyFunctionalDependencyResultReceiver();

}
