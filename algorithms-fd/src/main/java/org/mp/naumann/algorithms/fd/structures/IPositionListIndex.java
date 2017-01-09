package org.mp.naumann.algorithms.fd.structures;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Collection;

public interface IPositionListIndex {
    int getAttribute();

    Collection<IntArrayList> getClusters();
}
