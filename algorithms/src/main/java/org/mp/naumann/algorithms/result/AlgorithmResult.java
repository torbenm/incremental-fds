package org.mp.naumann.algorithms.result;

import org.mp.naumann.algorithms.data.IntermediateDataStructure;

public class AlgorithmResult<T, R extends IntermediateDataStructure> {

    private ResultSet<T> resultSet;
    private R intermediateDataStructure;

    public AlgorithmResult(){}

    public AlgorithmResult(ResultSet<T> resultSet) {
        this.resultSet = resultSet;
    }

    public AlgorithmResult(ResultSet<T> resultSet, R intermediateDataStructure) {
        this.resultSet = resultSet;
        this.intermediateDataStructure = intermediateDataStructure;
    }

    public ResultSet<T> getResultSet() {
        return resultSet;
    }

    public void setResultSet(ResultSet<T> resultSet) {
        this.resultSet = resultSet;
    }

    public R getIntermediateDataStructure() {
        return intermediateDataStructure;
    }

    public void setIntermediateDataStructure(R intermediateDataStructure) {
        this.intermediateDataStructure = intermediateDataStructure;
    }
}
