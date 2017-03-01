package org.mp.naumann.algorithms.fd.incremental.datastructures;

import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;

public class ArrayCompressedRecords implements CompressedRecords {

    private final int[][] compressedRecords;
    private final int numAttributes;

    public ArrayCompressedRecords(int[][] compressedRecords, int numAttributes) {
        this.compressedRecords = compressedRecords;
        this.numAttributes = numAttributes;
    }

    @Override
    public int[] get(int index) {
        return compressedRecords[index];
    }

    @Override
    public int getNumAttributes() {
        return numAttributes;
    }
}
