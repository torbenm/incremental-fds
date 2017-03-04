package org.mp.naumann.algorithms.fd.incremental;

public interface CompressedRecords {

    int[] get(int index);

    int getNumAttributes();
}
