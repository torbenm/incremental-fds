package org.mp.naumann.algorithms.fd;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.mp.naumann.algorithms.fd.structures.FDTree;

import java.util.HashMap;
import java.util.List;

public class FDIntermediateDatastructure {

	private final FDTree posCover;
	private final List<HashMap<String, IntArrayList>> clusterMaps;
	private final int numRecords;
	private final List<Integer> pliSequence;

	public FDIntermediateDatastructure(FDTree posCover, List<HashMap<String, IntArrayList>> clusterMaps, int numRecords, List<Integer> pliSequence) {
		this.posCover = posCover;
		this.clusterMaps = clusterMaps;
		this.numRecords = numRecords;
		this.pliSequence = pliSequence;
	}

	public FDTree getPosCover() {
		return posCover;
	}

	public List<HashMap<String, IntArrayList>> getClusterMaps() { return clusterMaps; }

	public int getNumRecords() { return numRecords; }

	public List<Integer> getPliSequence() {
		return pliSequence;
	}
}
