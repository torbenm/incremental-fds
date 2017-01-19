package org.mp.naumann.algorithms.fd.incremental.datastructures;

import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.utils.PliUtils;
import org.mp.naumann.processor.batch.Batch;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public interface DataStructureBuilder {

    CompressedDiff update(Batch batch);

    List<? extends PositionListIndex> getPlis();

    CompressedRecords getCompressedRecord();

    static Set<Integer> getClustersWithNewRecords(CompressedRecords compressedRecords, Collection<Integer> newRecords, int attribute) {
        Set<Integer> clusterIds = new HashSet<>();
        for (int id : newRecords) {
            int clusterId = compressedRecords.get(id)[attribute];
            if (clusterId != PliUtils.UNIQUE_VALUE) {
                clusterIds.add(clusterId);
            }
        }
        return clusterIds;
    }
}
