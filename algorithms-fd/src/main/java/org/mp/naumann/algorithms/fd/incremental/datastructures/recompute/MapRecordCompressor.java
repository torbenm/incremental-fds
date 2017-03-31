package org.mp.naumann.algorithms.fd.incremental.datastructures.recompute;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.datastructures.MapCompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.utils.PliUtils;

class MapRecordCompressor implements RecordCompressor {

    private final Set<Integer> recordIds;
    private final List<? extends PositionListIndex> plis;

    MapRecordCompressor(Set<Integer> recordIds, List<? extends PositionListIndex> plis) {
        this.recordIds = recordIds;
        this.plis = plis;
    }

    private static int[] fetchRecordFrom(int recordId, List<Map<Integer, Integer>> invertedPlis) {
        int numAttributes = invertedPlis.size();
        int[] record = new int[numAttributes];
        for (int i = 0; i < numAttributes; i++) {
            record[i] = invertedPlis.get(i).getOrDefault(recordId, PliUtils.UNIQUE_VALUE);
        }
        return record;
    }

    @Override
    public CompressedRecords buildCompressedRecords() {
        MapCompressedRecords compressedRecords = new MapCompressedRecords(recordIds.size(), plis.size());
        List<Map<Integer, Integer>> invertedPlis = invertPlis();
        for (int recordId : recordIds) {
            compressedRecords.put(recordId, fetchRecordFrom(recordId, invertedPlis));
        }
        return compressedRecords;
    }

    private List<Map<Integer, Integer>> invertPlis() {
        List<Map<Integer, Integer>> invertedPlis = new ArrayList<>();
        for (PositionListIndex pli : plis) {
            Map<Integer, Integer> invertedPli = new HashMap<>(recordIds.size());

            int clusterId = 0;
            for (Collection<Integer> cluster : pli.getClusters()) {
                for (int recordId : cluster) {
                    invertedPli.put(recordId, clusterId);
                }
                clusterId++;
            }
            invertedPlis.add(invertedPli);
        }
        return invertedPlis;
    }

}
