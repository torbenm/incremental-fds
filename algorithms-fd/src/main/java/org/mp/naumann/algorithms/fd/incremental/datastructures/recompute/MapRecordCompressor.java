package org.mp.naumann.algorithms.fd.incremental.datastructures.recompute;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntSet;
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

    private final IntSet recordIds;
    private final List<? extends PositionListIndex> plis;

    MapRecordCompressor(IntSet recordIds, List<? extends PositionListIndex> plis) {
        this.recordIds = recordIds;
        this.plis = plis;
    }

    private static int[] fetchRecordFrom(int recordId, List<Int2IntMap> invertedPlis) {
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
        List<Int2IntMap> invertedPlis = invertPlis();
        for (int recordId : recordIds) {
            compressedRecords.put(recordId, fetchRecordFrom(recordId, invertedPlis));
        }
        return compressedRecords;
    }

    private List<Int2IntMap> invertPlis() {
        List<Int2IntMap> invertedPlis = new ArrayList<>();
        for (PositionListIndex pli : plis) {
            Int2IntMap invertedPli = new Int2IntOpenHashMap(recordIds.size());

            int clusterId = 0;
            for (Cluster cluster : pli.getClusters()) {
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
