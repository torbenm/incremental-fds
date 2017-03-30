package org.mp.naumann.algorithms.fd.incremental.datastructures.recompute;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.datastructures.ArrayCompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.utils.PliUtils;

class ArrayRecordCompressor implements RecordCompressor {

    private final Set<Integer> recordIds;
    private final List<? extends PositionListIndex> plis;
    private final int numRecords;

    ArrayRecordCompressor(Set<Integer> recordIds, List<? extends PositionListIndex> plis, int numRecords) {
        this.recordIds = recordIds;
        this.plis = plis;
        this.numRecords = numRecords;
    }

    private static int[] fetchRecordFrom(int recordId, int[][] invertedPlis) {
        int numAttributes = invertedPlis.length;
        int[] record = new int[numAttributes];
        for (int i = 0; i < numAttributes; i++) {
            record[i] = invertedPlis[i][recordId];
        }
        return record;
    }

    @Override
    public CompressedRecords buildCompressedRecords() {
        int[][] compressedRecords = new int[numRecords][];
        int[][] invertedPlis = invertPlis();
        for (int recordId : recordIds) {
            compressedRecords[recordId] = fetchRecordFrom(recordId, invertedPlis);
        }
        return new ArrayCompressedRecords(compressedRecords, plis.size());
    }

    private int[][] invertPlis() {
        int[][] invertedPlis = new int[plis.size()][];
        int i = 0;
        for (PositionListIndex pli : plis) {
            int[] invertedPli = new int[numRecords];
            Arrays.fill(invertedPli, PliUtils.UNIQUE_VALUE);

            int clusterId = 0;
            for (Collection<Integer> cluster : pli.getClusters()) {
                for (int recordId : cluster) {
                    invertedPli[recordId] = clusterId;
                }
                clusterId++;
            }
            invertedPlis[i] = invertedPli;
            i++;
        }
        return invertedPlis;
    }

}
