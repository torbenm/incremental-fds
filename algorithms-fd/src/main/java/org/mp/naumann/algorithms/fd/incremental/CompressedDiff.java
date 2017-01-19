package org.mp.naumann.algorithms.fd.incremental;

import java.util.Arrays;
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

    public static CompressedDiff buildDiff(Collection<Integer> inserted, Collection<Integer> deleted, IncrementalFDConfiguration version, CompressedRecords compressedRecords) {
        int[][] insertedRecords = diffToArray(inserted, compressedRecords, version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.SIMPLE), false);
		int[][] deletedRecords = diffToArray(deleted, compressedRecords, version.usesPruningStrategy(IncrementalFDConfiguration.PruningStrategy.ANNOTATION), true);
		int[][] oldUpdatedRecords = new int[0][];
		int[][] newUpdatedRecords = new int[0][];
		return new CompressedDiff(insertedRecords, deletedRecords, oldUpdatedRecords, newUpdatedRecords);
	}

    private static int[][] diffToArray(Collection<Integer> diff, CompressedRecords compressedRecords, boolean fillWithData, boolean remove){
        int[][] array = new int[diff.size()][];
        if (fillWithData) {
            int i = 0;
            for (int id : diff) {
                // If we will invalidate the records just after getting them,
                // we must make sure to clone them because arrays are passed by-reference.
                array[i] = compressedRecords.get(id, remove);
                if(remove) {
                    compressedRecords.invalidate(id);
                }
                i++;
            }
        }
        return array;
    }

}
