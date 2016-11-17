package org.mp.naumann.algorithms.implementations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.mp.naumann.algorithms.IncrementalAlgorithm;
import org.mp.naumann.algorithms.result.ResultListener;
import org.mp.naumann.database.statement.DeleteStatement;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.UpdateStatement;
import org.mp.naumann.processor.batch.Batch;

public class AverageIncrementalAlgorithm implements IncrementalAlgorithm<Double, AverageDatastructure> {

	private final List<ResultListener<Double>> resultListeners = new ArrayList<>();
	private final String column;
	private AverageDatastructure ds = new AverageDatastructure();

	public AverageIncrementalAlgorithm(String column) {
		this.column = column;
	}

	@Override
	public Collection<ResultListener<Double>> getResultListeners() {
		return resultListeners;
	}

	@Override
	public void addResultListener(ResultListener<Double> listener) {
		this.resultListeners.add(listener);
	}

	@Override
	public Double execute(Batch batch) {
		List<DeleteStatement> deletes = batch.getDeleteStatements();
		List<UpdateStatement> updates = batch.getUpdateStatements();
		List<InsertStatement> inserts = batch.getInsertStatements();
		for (DeleteStatement delete : deletes) {
			ds.decreaseCount();
			String oldValue = delete.getValueMap().get(column);
			if (oldValue != null && !oldValue.isEmpty()) {
				ds.decreaseSum(Double.parseDouble(oldValue));
			}
		}
		for (InsertStatement insert : inserts) {
			ds.increaseCount();
			String newValue = insert.getValueMap().get(column);
			if (newValue != null && !newValue.isEmpty()) {
				ds.increaseSum(Double.parseDouble(newValue));
			}
		}
		for (UpdateStatement update : updates) {
			String newValue = update.getValueMap().get(column);
			if (newValue != null && !newValue.isEmpty()) {
				ds.increaseSum(Double.parseDouble(newValue));
			}
			String oldValue = update.getOldValueMap().get(column);
			if (oldValue != null && !oldValue.isEmpty()) {
				ds.decreaseSum(Double.parseDouble(oldValue));
			}
		}
		return ds.getAverage();
	}

	@Override
	public void setIntermediateDataStructure(AverageDatastructure intermediateDataStructure) {
		this.ds = intermediateDataStructure;
	}

}
