package org.mp.naumann.algorithms;

import org.mp.naumann.algorithms.data.IntermediateDataStructure;
import org.mp.naumann.algorithms.result.AlgorithmResult;
import org.mp.naumann.algorithms.result.ResultListener;
import org.mp.naumann.algorithms.result.ResultSet;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.handler.BatchHandler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class IncrementalAlgorithm implements Algorithm, BatchHandler {

    private ResultSet resultSet;
    private IntermediateDataStructure intermediateDataStructure;
    private final DataConnector dataConnector;
    private final Set<ResultListener> resultListeners = new HashSet<>();

    public IncrementalAlgorithm(DataConnector dataConnector, IntermediateDataStructure intermediateDataStructure) {
        this.dataConnector = dataConnector;
        this.intermediateDataStructure = intermediateDataStructure;
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

    @Override
    public void handleBatch(Batch batch) {
        AlgorithmResult result = execute(batch);
        for(ResultListener resultListener : resultListeners){
            resultListener.receiveResult(result);
        }
    }

    public void addResultListener(ResultListener listener){
        resultListeners.add(listener);
    }

    public abstract AlgorithmResult execute(Batch batch);
}
