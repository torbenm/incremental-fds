package org.mp.naumann.algorithms.fd.structures;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Map.Entry;

public interface IPositionListIndex {
    int getAttribute();

    Iterable<Entry<Integer, IntArrayList>> getClusterEntries();
    IntArrayList getCluster(int index);
}
