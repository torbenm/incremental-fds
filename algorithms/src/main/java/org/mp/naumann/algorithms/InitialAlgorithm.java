package org.mp.naumann.algorithms;

import org.mp.naumann.algorithms.data.IntermediateDataStructure;
import org.mp.naumann.algorithms.result.ResultSet;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.algorithms.result.AlgorithmResult;

public abstract class InitialAlgorithm implements Algorithm {

    private ResultSet resultSet;
    private IntermediateDataStructure intermediateDataStructure;
    private final DataConnector dataConnector;

    public InitialAlgorithm(DataConnector dataConnector) {
        this.dataConnector = dataConnector;
    }

    protected DataConnector getDataConnector() {
        return dataConnector;
    }

    protected void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    protected void setIntermediateDataStructure(IntermediateDataStructure intermediateDataStructure) {
        this.intermediateDataStructure = intermediateDataStructure;
    }

    @Override
    public ResultSet getResultSet() {
        return resultSet;
    }

    @Override
    public IntermediateDataStructure getIntermediateDataStructure() {
        return intermediateDataStructure;
    }

    public abstract AlgorithmResult execute();
}
