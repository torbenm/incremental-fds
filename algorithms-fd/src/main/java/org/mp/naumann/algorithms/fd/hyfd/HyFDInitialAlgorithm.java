package org.mp.naumann.algorithms.fd.hyfd;

import org.mp.naumann.algorithms.AlgorithmExecutionException;
import org.mp.naumann.algorithms.InitialAlgorithm;
import org.mp.naumann.algorithms.data.NoIntermediateDataStructure;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.result.AlgorithmResult;
import org.mp.naumann.algorithms.result.ListResultSet;
import org.mp.naumann.database.DataConnector;

import java.util.ArrayList;
import java.util.List;

public class HyFDInitialAlgorithm extends InitialAlgorithm<FunctionalDependency, NoIntermediateDataStructure> {

    private final String tableName;
    private final List<FunctionalDependency> functionalDependencies;

    public HyFDInitialAlgorithm(DataConnector dataConnector,String tableName) {
        super(dataConnector);
        this.tableName = tableName;
        this.functionalDependencies = new ArrayList<>();
    }


    @Override
    public AlgorithmResult<FunctionalDependency, NoIntermediateDataStructure> execute() {
        HyFD hyFD = new HyFD(getDataConnector().getTable(tableName),
                functionalDependencies::add);
        try {
            hyFD.execute();
            return new AlgorithmResult<>(new ListResultSet<>(functionalDependencies));
        } catch (AlgorithmExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed while executing algorithm!", e);
        }
    }
}
