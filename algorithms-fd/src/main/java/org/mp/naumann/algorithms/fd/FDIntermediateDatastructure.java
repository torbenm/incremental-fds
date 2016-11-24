package org.mp.naumann.algorithms.fd;

import java.util.HashMap;
import java.util.List;

import org.mp.naumann.algorithms.fd.structures.FDTree;

import it.unimi.dsi.fastutil.ints.IntArrayList;

public class FDIntermediateDatastructure {

	private final FDTree posCover;
	private final List<HashMap<String, IntArrayList>> clusterMaps;
	private final int numRecords;

	/*
	Do we even need the plis here when we have to recalculate them anyway after an update instruction?
	 */
	public FDIntermediateDatastructure(FDTree posCover, List<HashMap<String, IntArrayList>> clusterMaps, int numRecords) {
		this.posCover = posCover;
		this.clusterMaps = clusterMaps;
		this.numRecords = numRecords;
	}

	public FDTree getPosCover() {
		return posCover;
	}

	public List<HashMap<String, IntArrayList>> getClusterMaps() { return clusterMaps; }

	public int getNumRecords() { return numRecords; }
}
