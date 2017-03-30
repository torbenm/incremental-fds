package org.mp.naumann.algorithms.fd.incremental.datastructures.incremental;

import org.mp.naumann.algorithms.fd.utils.PliUtils;

import java.util.HashMap;
import java.util.Map;

class Dictionary<T> {

    private final int NULL;
    private final Map<T, Integer> dictionary = new HashMap<>();
    private int nextValue = 1;

    Dictionary() {
        this(false);
    }

    Dictionary(boolean isNullEqualNull) {
        NULL = isNullEqualNull ? 0 : PliUtils.UNIQUE_VALUE;
    }

    int getOrAdd(T value) {
        if (value == null) {
            return NULL;
        }
        return dictionary.computeIfAbsent(value, k -> nextValue++);
    }

}
