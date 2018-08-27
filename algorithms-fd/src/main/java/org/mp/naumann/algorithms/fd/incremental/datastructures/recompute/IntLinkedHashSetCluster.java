package org.mp.naumann.algorithms.fd.incremental.datastructures.recompute;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Collections;

public class IntLinkedHashSetCluster implements Cluster {

	private final IntLinkedOpenHashSet cluster = new IntLinkedOpenHashSet();

	@Override
	public void add(int i) {
		cluster.add(i);
	}

	@Override
	public void addAll(IntCollection integers) {
		cluster.addAll(integers);
	}

	@Override
	public boolean isEmpty() {
		return cluster.isEmpty();
	}

	@Override
	public IntIterator iterator() {
		return cluster.iterator();
	}

	@Override
	public int largestElement() {
		return cluster.lastInt();
	}

	@Override
	public void removeAll(IntCollection integers) {
		cluster.removeAll(integers);
	}

	@Override
	public int size() {
		return cluster.size();
	}

	@Override
	public IntCollection toCollection() {
		return cluster;
	}

	@Override
	public IntSet asSet() {
		return cluster;
	}
}
