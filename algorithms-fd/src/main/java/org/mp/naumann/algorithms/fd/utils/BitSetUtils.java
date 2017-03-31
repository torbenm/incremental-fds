package org.mp.naumann.algorithms.fd.utils;

import org.apache.lucene.util.OpenBitSet;

import java.util.ArrayList;
import java.util.List;

public class BitSetUtils {

    public static boolean isContained(OpenBitSet a, OpenBitSet b) {
        return OpenBitSet.andNotCount(a, b) == 0;
    }

    public static String toString(OpenBitSet a) {
        return toString(a, a.length());
    }

    public static List<Integer> collectSetBits(OpenBitSet bitSet) {
        List<Integer> cols = new ArrayList<>();
        for (int nextSetBit = bitSet.nextSetBit(0); nextSetBit >= 0; nextSetBit = bitSet.nextSetBit(nextSetBit + 1)) {
            cols.add(nextSetBit);
        }
        return cols;
    }

    public static String toString(OpenBitSet lhs, int numAttributes) {
        StringBuilder s = new StringBuilder();

        for (int i = 0; i < numAttributes; i++) {
            s.append(lhs.fastGet(i) ? 1 : 0);
        }
        return s.toString();
    }

    public static OpenBitSet fromString(String s) {
        OpenBitSet bits = new OpenBitSet(s.length());
        for (int idx = s.indexOf("1"); idx >= 0; idx = s.indexOf("1", idx + 1)) {
            bits.fastSet(idx);
        }
        return bits;
    }
}
