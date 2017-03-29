package org.mp.naumann.algorithms.fd.structures;

import org.mp.naumann.algorithms.fd.utils.PliUtils;

import java.util.HashMap;
import java.util.Map;

public class Dictionary<T> {

    private final int NULL;
    private final Map<T, Integer> dictionary = new HashMap<>();
    private int nextValue = 1;

    public Dictionary() {
        this(false);
    }

    public Dictionary(boolean isNullEqualNull) {
        NULL = isNullEqualNull ? 0 : PliUtils.UNIQUE_VALUE;
    }

    public int getOrAdd(T value) {
        if (value == null) {
            return NULL;
        }
        return dictionary.computeIfAbsent(value, k -> nextValue++);
    }

}
