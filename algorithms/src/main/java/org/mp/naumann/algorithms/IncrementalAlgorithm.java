package org.mp.naumann.algorithms;

import java.util.HashSet;
import java.util.Set;

import org.mp.naumann.algorithms.data.IntermediateDataStructure;
import org.mp.naumann.algorithms.result.AlgorithmResult;
import org.mp.naumann.algorithms.result.ResultListener;
import org.mp.naumann.algorithms.result.ResultSet;
import org.mp.naumann.database.DataConnector;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.handler.BatchHandler;

public abstract class IncrementalAlgorithm<T, R extends IntermediateDataStructure> implements Algorithm<T, R>, BatchHandler {

    private ResultSet<T> resultSet;
    private R intermediateDataStructure;
    private final DataConnector dataConnector;
    private final Set<ResultListener> resultListeners = new HashSet<>();

    public IncrementalAlgorithm(DataConnector dataConnector, R intermediateDataStructure) {
        this.dataConnector = dataConnector;
        this.intermediateDataStructure = intermediateDataStructure;
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

    public abstract AlgorithmResult<T, R> execute(Batch batch);
}
