package org.mp.naumann.algorithms.fd.utils;

import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.mp.naumann.algorithms.fd.incremental.datastructures.recompute.Cluster;

public class CollectionUtils {

    // Simply concatenate the elements of an IntArrayList
    public static String concat(Collection<Integer> integers, String separator) {
        if (integers == null)
            return "";

        StringBuilder buffer = new StringBuilder();

        for (int integer : integers) {
            buffer.append(integer);
            buffer.append(separator);
        }

        if (buffer.length() > separator.length())
            buffer.delete(buffer.length() - separator.length(), buffer.length());

        return buffer.toString();
    }

    public static IntSet intersection(List<Cluster> clusters) {
        if (clusters.isEmpty()) {
            return IntSets.EMPTY_SET;
        }
        clusters.sort(Comparator.comparingInt(Cluster::size));
        IntSet matching = null;
        for (Cluster cluster : clusters) {
            if (matching == null) {
                matching = new IntOpenHashSet(cluster.toCollection());
            } else {
                matching.retainAll(cluster.asSet());
            }
            if (matching.isEmpty()) {
                return matching;
            }
        }
        return matching;
    }
}
