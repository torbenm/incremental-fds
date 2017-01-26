package org.mp.naumann.algorithms.fd.incremental.datastructures;

import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;

import java.util.HashMap;
import java.util.Map;

public class MapCompressedRecords implements CompressedRecords {

    private final Map<Integer, int[]> compressedRecords = new HashMap<>();

    @Override
    public int[] get(int index) {
        return compressedRecords.get(index);
    }

    @Override
    public int size() {
        return compressedRecords.size();
    }

    @Override
    public int getNumAttributes() {
        return compressedRecords.values().iterator().next().length;
    }

    public void put(Integer id, int[] record) {
        compressedRecords.put(id, record);
    }
}
