package org.mp.naumann.algorithms;

import org.mp.naumann.algorithms.data.IntermediateDataStructure;
import org.mp.naumann.algorithms.result.ResultSet;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.algorithms.result.AlgorithmResult;

public abstract class InitialAlgorithm<T, R extends IntermediateDataStructure> implements Algorithm<T, R> {

    private ResultSet<T> resultSet;
    private R intermediateDataStructure;
    private final DataConnector dataConnector;

    public InitialAlgorithm(DataConnector dataConnector) {
        this.dataConnector = dataConnector;
    }

    protected DataConnector getDataConnector() {
        return dataConnector;
    }

    protected void setResultSet(ResultSet<T> resultSet) {
        this.resultSet = resultSet;
    }

    protected void setIntermediateDataStructure(R intermediateDataStructure) {
        this.intermediateDataStructure = intermediateDataStructure;
    }

    @Override
    public ResultSet<T> getResultSet() {
        return resultSet;
    }

    @Override
    public R getIntermediateDataStructure() {
        return intermediateDataStructure;
    }

    public abstract AlgorithmResult<T, R> execute();
}
