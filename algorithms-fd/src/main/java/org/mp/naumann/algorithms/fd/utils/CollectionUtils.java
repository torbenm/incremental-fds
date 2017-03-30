package org.mp.naumann.algorithms.fd.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    public static <T> Set<T> intersection(List<Collection<T>> clusters) {
        if (clusters.isEmpty()) {
            return Collections.emptySet();
        }
        clusters.sort(Comparator.comparingInt(Collection::size));
        Set<T> matching = null;
        for (Collection<T> cluster : clusters) {
            if (matching == null) {
                matching = new HashSet<>(cluster);
            } else {
                matching.retainAll(cluster);
            }
            if (matching.isEmpty()) {
                return matching;
            }
        }
        return matching;
    }
}
