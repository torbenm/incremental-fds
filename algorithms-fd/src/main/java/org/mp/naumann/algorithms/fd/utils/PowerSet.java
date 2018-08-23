package org.mp.naumann.algorithms.fd.utils;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PowerSet {

    public static Set<IntSet> getPowerSet(IntSet originalSet, int maxSize) {
        Set<IntSet> powerSet = new HashSet<>();
        if (originalSet.isEmpty()) {
            powerSet.add(new IntOpenHashSet());
            return powerSet;
        }
        IntList list = new IntArrayList(originalSet);
        int head = list.getInt(0);
        IntSet rest = new IntOpenHashSet(list.subList(1, list.size()));
        for (IntSet set : getPowerSet(rest, maxSize)) {
            IntSet newSet = new IntOpenHashSet();
            newSet.add(head);
            newSet.addAll(set);
            if (newSet.size() <= maxSize) {
                powerSet.add(newSet);
            }
            if (set.size() <= maxSize) {
                powerSet.add(set);
            }
        }
        return powerSet;
    }

}
