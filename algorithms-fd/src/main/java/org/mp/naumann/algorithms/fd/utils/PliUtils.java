package org.mp.naumann.algorithms.fd.utils;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.mp.naumann.algorithms.fd.structures.IPositionListIndex;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

public class PliUtils {

    public static final int UNIQUE_VALUE = -1;

    public static int[][] invert(List<? extends IPositionListIndex> plis, int numRecords) {
        int[][] invertedPlis = new int[plis.size()][];
        for (int attr = 0; attr < plis.size(); attr++) {
            int[] invertedPli = new int[numRecords];
            Arrays.fill(invertedPli, UNIQUE_VALUE);

            for (Entry<Integer, IntArrayList> cluster : plis.get(attr).getClusterEntries()) {
                for (int recordId : cluster.getValue()) {
                    invertedPli[recordId] = cluster.getKey();
                }
            }
            invertedPlis[attr] = invertedPli;
        }
        return invertedPlis;
    }
}
