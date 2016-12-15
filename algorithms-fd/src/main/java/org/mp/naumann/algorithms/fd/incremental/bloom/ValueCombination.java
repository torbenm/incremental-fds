package org.mp.naumann.algorithms.fd.incremental.bloom;

import org.mp.naumann.algorithms.fd.utils.PowerSet;

import java.util.HashSet;
import java.util.Set;

public class ValueCombination {

	private final Set<ColumnValue> values = new HashSet<>();

	public ValueCombination add(String columnName, String value) {
		values.add(new ColumnValue(columnName, value));
		return this;
	}

	public Set<Set<ColumnValue>> getPowerSet(int maxSize) {
		return PowerSet.getPowerSet(values, maxSize);
	}

}
