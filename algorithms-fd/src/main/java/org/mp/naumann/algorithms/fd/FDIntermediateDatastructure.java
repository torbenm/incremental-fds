package org.mp.naumann.algorithms.fd;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.PositionListIndex;

public class FDIntermediateDatastructure {

	private final FDTree posCover;
	private final List<Set<String>> columnValues;
	private final List<PositionListIndex> plis;
	private final int[][] compressedRecords;
	private final List<HashMap<String, IntArrayList>> clusterMaps;
	private final int numRecords;

	/*
	Do we even need the plis here when we have to recalculate them anyway after an update instruction?
	 */
	public FDIntermediateDatastructure(FDTree posCover, List<Set<String>> columnValues, List<PositionListIndex> plis, int[][] compressedRecords, List<HashMap<String, IntArrayList>> clusterMaps, int numRecords) {
		this.posCover = posCover;
		this.columnValues = columnValues;
		this.plis = plis;
		this.compressedRecords = compressedRecords;
		this.clusterMaps = clusterMaps;
		this.numRecords = numRecords;
	}

	public FDTree getPosCover() {
		return posCover;
	}

	public List<Set<String>> getColumnValues() {
		return columnValues;
	}

	public List<PositionListIndex> getPlis() {
		return plis;
	}

	public int[][] getCompressedRecords() {
		return compressedRecords;
	}

	public List<HashMap<String, IntArrayList>> getClusterMaps() { return clusterMaps; }

	public int getNumRecords() { return numRecords; }
}
