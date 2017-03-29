package org.mp.naumann.algorithms.fd;

import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.database.Table;

public interface FunctionalDependencyAlgorithm {
    void execute() throws AlgorithmExecutionException;

    void configure(Table table, FunctionalDependencyResultReceiver resultReceiver);

}
