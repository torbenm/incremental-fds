package org.mp.naumann.algorithms.fd.structures;

import java.util.HashMap;
import java.util.Map;

public class Dictionary<T> {

    public static final int NULL = 0;
    private final Map<T, Integer> dictionary = new HashMap<>();
    private int nextValue = 1;

    public int getOrAdd(T value) {
        if (value == null) {
            return NULL;
        }
        Integer dictValue = dictionary.get(value);
        if (dictValue == null) {
            dictValue = nextValue++;
            dictionary.put(value, dictValue);
        }
        return dictValue;
    }

    public Map<T, Integer> getMap() {
        return dictionary;
    }
}
