package org.mp.naumann.algorithms.fd.incremental.datastructures;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Iterator;

public interface ClusterFilter {

    Iterator<IntArrayList> clusters();

}
