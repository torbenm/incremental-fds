package org.mp.naumann.algorithms.fd.incremental;

import java.util.Map;

public class CompressedDiff {

    private final Map<Integer, int[]> insertedRecords;
    private final Map<Integer, int[]> deletedRecords;
    private final Map<Integer, int[]> oldUpdatedRecords;
    private final Map<Integer, int[]> newUpdatedRecords;

    public CompressedDiff(Map<Integer, int[]> insertedRecords, Map<Integer, int[]> deletedRecords, Map<Integer, int[]> oldUpdatedRecords,
                          Map<Integer, int[]> newUpdatedRecords) {
        this.insertedRecords = insertedRecords;
        this.deletedRecords = deletedRecords;
        this.oldUpdatedRecords = oldUpdatedRecords;
        this.newUpdatedRecords = newUpdatedRecords;
    }

    boolean hasInserts() {
        return !(insertedRecords.isEmpty() && newUpdatedRecords.isEmpty());
    }

    boolean hasDeletes() {
        return !(deletedRecords.isEmpty() && oldUpdatedRecords.isEmpty());
    }

    public Map<Integer, int[]> getInsertedRecords() {
        return insertedRecords;
    }

    public Map<Integer, int[]> getDeletedRecords() {
        return deletedRecords;
    }

    public Map<Integer, int[]> getOldUpdatedRecords() {
        return oldUpdatedRecords;
    }

    public Map<Integer, int[]> getNewUpdatedRecords() {
        return newUpdatedRecords;
    }

}
