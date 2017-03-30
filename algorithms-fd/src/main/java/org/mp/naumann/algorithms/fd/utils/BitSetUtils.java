package org.mp.naumann.algorithms.fd.utils;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
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

    public static OpenBitSet generateOpenBitSet(int... setBits) {
        OpenBitSet obs = new OpenBitSet();
        for (int setBit : setBits) {
            obs.set(setBit);
        }
        return obs;
    }

    public static Iterable<Integer> iterable(OpenBitSet obs) {
        return () -> new Iterator<Integer>() {
            private int rhsAttr = obs.nextSetBit(0);

            @Override
            public boolean hasNext() {
                return rhsAttr >= 0;
            }

            @Override
            public Integer next() {
                int tmp = rhsAttr;
                rhsAttr = obs.nextSetBit(rhsAttr + 1);
                return tmp;
            }
        };
    }

    public static boolean isEqual(OpenBitSet a1, OpenBitSet a2) {
        for (int i = 0; i < a1.capacity(); i++) {
            if (a1.get(i) != a2.get(i))
                return false;
        }
        return true;
    }

    public static Collection<OpenBitSetFD> toOpenBitSetFDCollection(OpenBitSet bitSet, int numAttributes) {
        Collection<OpenBitSetFD> openBitSetFDS = new ArrayList<>();
        OpenBitSet rhsFlipped = bitSet.clone();
        rhsFlipped.flip(0, numAttributes);
        for (int rhs : BitSetUtils.iterable(rhsFlipped)) {
            openBitSetFDS.add(new OpenBitSetFD(bitSet.clone(), rhs));
        }
        return openBitSetFDS;
    }

    public static OpenBitSet generateAllOnesBitSet(int numAttributes) {
        OpenBitSet obs = new OpenBitSet();
        obs.flip(0, numAttributes);
        return obs;
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
