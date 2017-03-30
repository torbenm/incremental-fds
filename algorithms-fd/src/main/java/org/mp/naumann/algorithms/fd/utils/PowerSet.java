package org.mp.naumann.algorithms.fd.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PowerSet {

    public static <T> Set<Set<T>> getPowerSet(Set<T> originalSet, int maxSize) {
        Set<Set<T>> powerSet = new HashSet<>();
        if (originalSet.isEmpty()) {
            powerSet.add(new HashSet<>());
            return powerSet;
        }
        List<T> list = new ArrayList<>(originalSet);
        T head = list.get(0);
        Set<T> rest = new HashSet<>(list.subList(1, list.size()));
        for (Set<T> set : getPowerSet(rest, maxSize)) {
            Set<T> newSet = new HashSet<>();
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
