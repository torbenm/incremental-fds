package org.mp.naumann.algorithms.fd.utils;

import org.mp.naumann.algorithms.fd.structures.PositionListIndex;

import java.util.Arrays;
import java.util.List;

public class PliUtils {

    public static int[][] invert(List<PositionListIndex> plis, int numRecords) {
        int[][] invertedPlis = new int[plis.size()][];
        for (int attr = 0; attr < plis.size(); attr++) {
            int[] invertedPli = new int[numRecords];
            Arrays.fill(invertedPli, -1);

            for (int clusterId = 0; clusterId < plis.get(attr).size(); clusterId++) {
                for (int recordId : plis.get(attr).getClusters().get(clusterId))
                    invertedPli[recordId] = clusterId;
            }
            invertedPlis[attr] = invertedPli;
        }
        return invertedPlis;
    }
}
