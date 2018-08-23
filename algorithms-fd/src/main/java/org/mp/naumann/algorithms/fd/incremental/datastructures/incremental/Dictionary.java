package org.mp.naumann.algorithms.fd.incremental.datastructures.incremental;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.mp.naumann.algorithms.fd.utils.PliUtils;

import java.util.HashMap;
import java.util.Map;

class Dictionary<T> {

    private final int NULL;
    private final Object2IntMap<T> dictionary = new Object2IntOpenHashMap<>();
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
        return dictionary.computeIntIfAbsent(value, k -> nextValue++);
    }

}
