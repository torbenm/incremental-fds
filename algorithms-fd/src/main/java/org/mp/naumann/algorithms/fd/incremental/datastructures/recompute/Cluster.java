package org.mp.naumann.algorithms.fd.incremental.datastructures.recompute;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntIterable;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Set;

public interface Cluster extends IntIterable {

	void add(int i);

	void addAll(IntCollection integers);

	default void addAll(Cluster integers) {
		addAll(integers.toCollection());
	}

	boolean isEmpty();

	@Override
	IntIterator iterator();

	int largestElement();

	void removeAll(IntCollection integers);

	int size();

	IntCollection toCollection();

	IntSet asSet();
}
