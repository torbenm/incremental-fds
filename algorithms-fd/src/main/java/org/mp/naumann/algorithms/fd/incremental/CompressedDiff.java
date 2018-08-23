package org.mp.naumann.algorithms.fd.incremental;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.util.Map;

public class CompressedDiff {

    private final Int2ObjectMap<int[]> insertedRecords;
    private final Int2ObjectMap<int[]> deletedRecords;
    private final Int2ObjectMap<int[]> oldUpdatedRecords;
    private final Int2ObjectMap<int[]> newUpdatedRecords;

    public CompressedDiff(Int2ObjectMap<int[]> insertedRecords, Int2ObjectMap<int[]> deletedRecords, Int2ObjectMap<int[]> oldUpdatedRecords,
                          Int2ObjectMap<int[]> newUpdatedRecords) {
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

    public Int2ObjectMap<int[]> getInsertedRecords() {
        return insertedRecords;
    }

    public Int2ObjectMap<int[]> getDeletedRecords() {
        return deletedRecords;
    }

    public Int2ObjectMap<int[]> getOldUpdatedRecords() {
        return oldUpdatedRecords;
    }

    public Int2ObjectMap<int[]> getNewUpdatedRecords() {
        return newUpdatedRecords;
    }

}
