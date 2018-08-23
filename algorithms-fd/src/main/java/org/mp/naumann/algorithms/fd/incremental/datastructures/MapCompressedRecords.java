package org.mp.naumann.algorithms.fd.incremental.datastructures;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;

import java.util.HashMap;
import java.util.Map;

public class MapCompressedRecords implements CompressedRecords {

    private final Int2ObjectMap<int[]> compressedRecords;
    private final int numAttributes;

    private MapCompressedRecords(Int2ObjectMap<int[]> compressedRecords, int numAttributes) {
        this.compressedRecords = compressedRecords;
        this.numAttributes = numAttributes;
    }

    public MapCompressedRecords(int initialSize, int numAttributes) {
        this(new Int2ObjectOpenHashMap<>(initialSize), numAttributes);
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

    public void put(int id, int[] record) {
        compressedRecords.put(id, record);
    }

    public void remove(int recordId) {
        compressedRecords.remove(recordId);
    }
}
