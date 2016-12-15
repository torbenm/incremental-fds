package org.mp.naumann.algorithms.fd;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;

import java.util.HashMap;
import java.util.List;

public class FDIntermediateDatastructure {

	private final FDTree posCover;
	private final List<HashMap<String, IntArrayList>> clusterMaps;
	private final int numRecords;
	private final List<Integer> pliSequence;
	private final FDSet negCover;
	private final ValueComparator valueComparator;

	public FDIntermediateDatastructure(FDSet negCover, FDTree posCover, List<HashMap<String, IntArrayList>> clusterMaps, int numRecords, List<Integer> pliSequence, ValueComparator valueComparator) {
		this.posCover = posCover;
		this.clusterMaps = clusterMaps;
		this.numRecords = numRecords;
		this.pliSequence = pliSequence;
		this.negCover = negCover;
		this.valueComparator = valueComparator;
	}

	public FDTree getPosCover() {
		return posCover;
	}

	public List<HashMap<String, IntArrayList>> getClusterMaps() { return clusterMaps; }

	public int getNumRecords() { return numRecords; }

	public List<Integer> getPliSequence() {
		return pliSequence;
	}

	public FDSet getNegCover() {
		return negCover;
	}

	public ValueComparator getValueComparator() {
		return valueComparator;
	}
}
