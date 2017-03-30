package org.mp.naumann.algorithms.fd.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.mp.naumann.algorithms.fd.hyfd.PositionListIndex;

public class PliUtils {

    public static final int UNIQUE_VALUE = -1;

    public static int[][] invert(List<PositionListIndex> plis, int numRecords) {
        int[][] invertedPlis = new int[plis.size()][];
        for (int attr = 0; attr < plis.size(); attr++) {
            int[] invertedPli = new int[numRecords];
            Arrays.fill(invertedPli, -1);

            int clusterId = 0;
            for (Collection<Integer> cluster : plis.get(attr).getClusters()) {
                for (int recordId : cluster) {
                    invertedPli[recordId] = clusterId;
                }
                clusterId++;
            }
            invertedPlis[attr] = invertedPli;
        }
        return invertedPlis;
    }
}
