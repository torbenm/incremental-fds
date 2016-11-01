package org.mp.naumann.algorithms.result;

import org.mp.naumann.algorithms.data.IntermediateDataStructure;

public class AlgorithmResult {

    private ResultSet resultSet;
    private IntermediateDataStructure intermediateDataStructure;

    public ResultSet getResultSet() {
        return resultSet;
    }

    public void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public IntermediateDataStructure getIntermediateDataStructure() {
        return intermediateDataStructure;
    }

    public void setIntermediateDataStructure(IntermediateDataStructure intermediateDataStructure) {
        this.intermediateDataStructure = intermediateDataStructure;
    }
}
