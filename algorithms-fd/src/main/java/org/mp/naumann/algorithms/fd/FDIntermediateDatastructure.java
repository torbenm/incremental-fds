package org.mp.naumann.algorithms.fd;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.ValueCombination.ColumnValue;

import com.google.common.hash.BloomFilter;

import it.unimi.dsi.fastutil.ints.IntArrayList;

public class FDIntermediateDatastructure {

	private final FDTree posCover;
	private final List<HashMap<String, IntArrayList>> clusterMaps;
	private final int numRecords;
	private final List<Integer> pliSequence;
	private final BloomFilter<Set<ColumnValue>> filter;

	public FDIntermediateDatastructure(FDTree posCover, List<HashMap<String, IntArrayList>> clusterMaps, int numRecords, List<Integer> pliSequence, BloomFilter<Set<ColumnValue>> filter) {
		this.posCover = posCover;
		this.clusterMaps = clusterMaps;
		this.numRecords = numRecords;
		this.pliSequence = pliSequence;
		this.filter = filter;
	}

	public FDTree getPosCover() {
		return posCover;
	}

	public List<HashMap<String, IntArrayList>> getClusterMaps() { return clusterMaps; }

	public int getNumRecords() { return numRecords; }

	public List<Integer> getPliSequence() {
		return pliSequence;
	}

	public BloomFilter<Set<ColumnValue>> getFilter() {
		return filter;
	}
}
