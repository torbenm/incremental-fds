package org.mp.naumann.algorithms.fd.incremental;

import java.util.Collection;

public class CompressedDiff {

	private final int[][] insertedRecords;
	private final int[][] deletedRecords;
	private final int[][] oldUpdatedRecords;
	private final int[][] newUpdatedRecords;

	private CompressedDiff(int[][] insertedRecords, int[][] deletedRecords, int[][] oldUpdatedRecords,
						   int[][] newUpdatedRecords) {
		this.insertedRecords = insertedRecords;
		this.deletedRecords = deletedRecords;
		this.oldUpdatedRecords = oldUpdatedRecords;
		this.newUpdatedRecords = newUpdatedRecords;
	}

	
	public int[][] getInsertedRecords() {
		return insertedRecords;
	}

	
	public int[][] getDeletedRecords() {
		return deletedRecords;
	}

	
	public int[][] getOldUpdatedRecords() {
		return oldUpdatedRecords;
	}

	
	public int[][] getNewUpdatedRecords() {
		return newUpdatedRecords;
	}

	public static CompressedDiff buildDiff(Collection<Integer> inserted, IncrementalFDConfiguration version, CompressedRecords compressedRecords) {
		int[][] insertedRecords = new int[inserted.size()][];
		if (version.getPruningStrategies().contains(IncrementalFDConfiguration.PruningStrategy.SIMPLE)) {
			int i = 0;
			for (int id : inserted) {
				insertedRecords[i] = compressedRecords.get(id);
				i++;
			}
		}
		int[][] deletedRecords = new int[0][];
		int[][] oldUpdatedRecords = new int[0][];
		int[][] newUpdatedRecords = new int[0][];
		return new CompressedDiff(insertedRecords, deletedRecords, oldUpdatedRecords, newUpdatedRecords);
	}

}
