package org.mp.naumann.algorithms.fd;

import java.util.List;
import java.util.Set;

import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.PositionListIndex;

public class FDIntermediateDatastructure {

	private final FDTree posCover;
	private final List<Set<String>> columnValues;
	private final List<PositionListIndex> plis;
	private final int[][] compressedRecords;

	public FDTree getPosCover() {
		return posCover;
	}

	public List<Set<String>> getColumnValues() {
		return columnValues;
	}

	public List<PositionListIndex> getPlis() {
		return plis;
	}

	public FDIntermediateDatastructure(FDTree posCover, List<Set<String>> columnValues, List<PositionListIndex> plis, int[][] compressedRecords) {
		this.posCover = posCover;
		this.columnValues = columnValues;
		this.plis = plis;
		this.compressedRecords = compressedRecords;
	}

	public int[][] getCompressedRecords() {
		return compressedRecords;
	}

}
