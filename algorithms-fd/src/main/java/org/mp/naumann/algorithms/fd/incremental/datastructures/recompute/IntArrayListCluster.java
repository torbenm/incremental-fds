package org.mp.naumann.algorithms.fd.incremental.datastructures.recompute;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;

public class IntArrayListCluster implements Cluster {

	private final IntList cluster = new IntArrayList();

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
		return cluster.getInt(cluster.size() - 1);
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
}
