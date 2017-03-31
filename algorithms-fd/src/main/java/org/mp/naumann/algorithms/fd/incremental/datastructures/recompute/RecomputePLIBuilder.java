/*
 * Copyright 2014 by the Metanome project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mp.naumann.algorithms.fd.incremental.datastructures.recompute;

import org.mp.naumann.algorithms.fd.incremental.Factory;
import org.mp.naumann.algorithms.fd.structures.ClusterMapBuilder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

class RecomputePLIBuilder {

    private final RecomputeClusterMapBuilder clusterMapBuilder;
    private final boolean isNullEqualNull;
    private final List<Integer> pliOrder;

    RecomputePLIBuilder(ClusterMapBuilder clusterMapBuilder, boolean isNullEqualNull,
        List<Integer> pliOrder,
        Factory<Collection<Integer>> clusterFactory) {
        this.clusterMapBuilder = new RecomputeClusterMapBuilder(clusterMapBuilder, clusterFactory);
        this.isNullEqualNull = isNullEqualNull;
        this.pliOrder = pliOrder;
    }

    /**
     * Creates the actual positionListIndices based on the clusterMaps calculated beforehand.
     * Clusters of size 1 are being discarded in the process.
     *
     * @return clustersPerAttribute,
     */
    List<ListPositionListIndex> fetchPositionListIndexes() {
        List<Map<String, Collection<Integer>>> clusterMaps = clusterMapBuilder.getClusterMaps();
        List<ListPositionListIndex> clustersPerAttribute = new ArrayList<>();
        for (int columnId : pliOrder) {
            List<Collection<Integer>> clusters = new ArrayList<>();
            Map<String, Collection<Integer>> clusterMap = clusterMaps.get(columnId);

            if (!isNullEqualNull)
                clusterMap.remove(null);

            for (Collection<Integer> cluster : clusterMap.values())
                if (cluster.size() > 1)
                    clusters.add(cluster);

            clustersPerAttribute.add(new ListPositionListIndex(columnId, clusters));
        }
        return clustersPerAttribute;
    }

    int addRecord(List<String> values) {
        return clusterMapBuilder.addRecord(values);
    }

    public int getNumRecords() {
        return clusterMapBuilder.getNumLastRecords();
    }

    public Collection<Integer> removeRecord(Iterable<String> values) {
        return clusterMapBuilder.removeRecord(values);
    }
}
