package org.mp.naumann.algorithms.fd.incremental;

public interface CompressedRecords {

    int[] get(int index);
    default int[] get(int index, boolean clone){
        return clone ? clone(index) : get(index);
    }
    default int[] clone(int index){
        return get(index).clone();
    }

    /**
     * Fills the complete record with the same value
     * @param index The index of the record
     * @param value The value to fill the record with
     */
    void fill(int index, int value);

    default void invalidate(int index) {
        fill(index, -1);
    }

    int size();
}
