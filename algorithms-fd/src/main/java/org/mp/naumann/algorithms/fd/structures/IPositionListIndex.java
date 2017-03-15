package org.mp.naumann.algorithms.fd.structures;

import java.util.Collection;

public interface IPositionListIndex {
    int getAttribute();

    Collection<? extends Collection<Integer>> getClusters();
    Collection<Integer> getCluster(int index);
}
