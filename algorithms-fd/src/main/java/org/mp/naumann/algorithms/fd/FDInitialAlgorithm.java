package org.mp.naumann.algorithms.fd;

import org.mp.naumann.algorithms.AlgorithmExecutionException;
import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.fd.fdep.FDEPExecutor;
import org.mp.naumann.algorithms.fd.hyfd.HyFD;
import org.mp.naumann.algorithms.fd.tane.TaneAlgorithm;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.Table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FDInitialAlgorithm implements InitialAlgorithm<List<FunctionalDependency>, Object> {

    private final List<String> algorithms = Arrays.asList("hyfd", "tane", "fdep");

    private final List<FunctionalDependency> functionalDependencies;
    private FunctionalDependencyAlgorithm fdAlgorithm;


    public FDInitialAlgorithm(String algorithm, DataConnector dataConnector, String tableName) {
        if(!algorithms.contains(algorithm.toLowerCase()))
            throw new RuntimeException("Unknown Algorithm "+algorithm);
        this.functionalDependencies = new ArrayList<>();
        initializeAlgorithm(algorithm, dataConnector, tableName);
    }

    private void initializeAlgorithm(String algorithm, DataConnector dataConnector, String tableName) {
        Table table = dataConnector.getTable(tableName);
        FunctionalDependencyResultReceiver resultReceiver = functionalDependencies::add;
        switch(algorithm.toLowerCase()){
            case "hyfd":
                fdAlgorithm = new HyFD(table, resultReceiver);
                break;
            case "fdep":
                fdAlgorithm = new FDEPExecutor(table, resultReceiver);
                break;
            case "tane":
                fdAlgorithm = new TaneAlgorithm(table, resultReceiver);
                break;
        }
    }

    @Override
    public List<FunctionalDependency> execute() {
        try {
            fdAlgorithm.execute();
            return functionalDependencies;
        } catch (AlgorithmExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed while executing algorithm!", e);
        }
    }

	@Override
	public Object getIntermediateDataStructure() {
		return null;
	}
}
