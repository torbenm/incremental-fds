package org.mp.naumann.algorithms.fd.incremental.datastructures;

import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;

import java.util.HashMap;
import java.util.Map;

public class MapCompressedRecords implements CompressedRecords {

    private final Map<Integer, int[]> compressedRecords;
    private final int numAttributes;

    private MapCompressedRecords(Map<Integer, int[]> compressedRecords, int numAttributes) {
        this.compressedRecords = compressedRecords;
        this.numAttributes = numAttributes;
    }

    public MapCompressedRecords(int initialSize, int numAttributes) {
        this(new HashMap<>(initialSize), numAttributes);
    }

    @Override
    public int[] get(int index) {
        return compressedRecords.get(index);
    }

    public int size() {
        return compressedRecords.size();
    }

    @Override
    public int getNumAttributes() {
        return numAttributes;
    }

    public void put(Integer id, int[] record) {
        compressedRecords.put(id, record);
    }

    public void remove(int recordId) {
        compressedRecords.remove(recordId);
    }
}
