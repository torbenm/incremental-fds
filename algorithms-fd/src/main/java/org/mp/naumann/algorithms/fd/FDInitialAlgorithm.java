package org.mp.naumann.algorithms.fd;

import org.mp.naumann.algorithms.AlgorithmExecutionException;
import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.data.NoIntermediateDataStructure;
import org.mp.naumann.algorithms.fd.fdep.FDEPExecutor;
import org.mp.naumann.algorithms.fd.hyfd.HyFD;
import org.mp.naumann.algorithms.fd.tane.TaneAlgorithm;
import org.mp.naumann.algorithms.result.AlgorithmResult;
import org.mp.naumann.algorithms.result.ListResultSet;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.database.Table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FDInitialAlgorithm extends InitialAlgorithm<FunctionalDependency, NoIntermediateDataStructure> {

    private final List<String> algorithms = Arrays.asList("hyfd", "tane", "fdep");

    private final String tableName;
    private final List<FunctionalDependency> functionalDependencies;
    private FunctionalDependencyAlgorithm fdAlgorithm;


    public FDInitialAlgorithm(String algorithm, DataConnector dataConnector, String tableName) {
        super(dataConnector);
        if(!algorithms.contains(algorithm.toLowerCase()))
            throw new RuntimeException("Unknown Algorithm "+algorithm);
        this.tableName = tableName;
        this.functionalDependencies = new ArrayList<>();
        initializeAlgorithm(algorithm, dataConnector, tableName);
    }

    private void initializeAlgorithm(String algorithm, DataConnector dataConnector, String tableName) {
        Table table = getDataConnector().getTable(tableName);
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
    public AlgorithmResult<FunctionalDependency, NoIntermediateDataStructure> execute() {
        try {
            fdAlgorithm.execute();
            return new AlgorithmResult<>(new ListResultSet<>(functionalDependencies));
        } catch (AlgorithmExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed while executing algorithm!", e);
        }
    }
}
