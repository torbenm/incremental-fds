package org.mp.naumann.algorithms.fd.incremental;

public class CompressedDiff {

	private final int[][] insertedRecords;
	private final int[][] deletedRecords;
	private final int[][] oldUpdatedRecords;
	private final int[][] newUpdatedRecords;

	public CompressedDiff(int[][] insertedRecords, int[][] deletedRecords, int[][] oldUpdatedRecords,
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

}
