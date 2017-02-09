package org.mp.naumann.algorithms.fd.incremental;

public interface CompressedRecords {

    int[] get(int index);
    default int[] get(int index, boolean clone){
        return clone ? clone(index) : get(index);
    }
    default int[] clone(int index){
        return get(index).clone();
    }

    int size();

    int getNumAttributes();
}
