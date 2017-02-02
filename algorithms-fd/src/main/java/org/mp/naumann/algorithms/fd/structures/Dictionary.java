package org.mp.naumann.algorithms.fd.structures;

import java.util.HashMap;
import java.util.Map;

public class Dictionary<T> {

    private static final int NULL = 0;
    private final Map<T, Integer> dictionary = new HashMap<>();
    private int nextValue = 1;
    private final boolean isNullEqualNull;

    public Dictionary() {
        this(false);
    }

    public Dictionary(boolean isNullEqualNull) {
        this.isNullEqualNull = isNullEqualNull;
    }

    public Integer getOrAdd(T value) {
        if (value == null) {
            return isNullEqualNull? NULL : null;
        }
        Integer dictValue = dictionary.get(value);
        if (dictValue == null) {
            dictValue = nextValue++;
            dictionary.put(value, dictValue);
        }
        return dictValue;
    }

}
