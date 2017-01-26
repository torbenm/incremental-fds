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

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.ClusterMapBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

class RecomputePLIBuilder {

    private final ClusterMapBuilder clusterMapBuilder;
    private final boolean isNullEqualNull;
    private final List<Integer> pliOrder;

    RecomputePLIBuilder(ClusterMapBuilder clusterMapBuilder, boolean isNullEqualNull, List<Integer> pliOrder) {
        this.clusterMapBuilder = clusterMapBuilder;
        this.isNullEqualNull = isNullEqualNull;
        this.pliOrder = pliOrder;
    }

    /**
     * Creates the actual positionListIndices based on the clusterMaps calculated beforehand.
     * Clusters of size 1 are being discarded in the process.
     *
     * @return clustersPerAttribute,
     */
    List<PositionListIndex> fetchPositionListIndexes() {
        SpeedBenchmark.begin(BenchmarkLevel.OPERATION);
        List<HashMap<Integer, IntArrayList>> clusterMaps = clusterMapBuilder.getClusterMaps();
        List<PositionListIndex> clustersPerAttribute = new ArrayList<>();
        for (int columnId : pliOrder) {
            Map<Integer, IntArrayList> clusters = new HashMap<>();
            HashMap<Integer, IntArrayList> clusterMap = clusterMaps.get(columnId);

            if (!isNullEqualNull)
                clusterMap.remove(null);

            for (Entry<Integer, IntArrayList> cluster : clusterMap.entrySet())
                if (cluster.getValue().size() > 1)
                    clusters.put(cluster.getKey(), cluster.getValue());

            clustersPerAttribute.add(new PositionListIndex(columnId, isNullEqualNull, clusters));
        }
        return clustersPerAttribute;
    }

    int addRecord(List<String> values) {
        return clusterMapBuilder.addRecord(values);
    }

    public int getNumRecords() {
        return clusterMapBuilder.getNumLastRecords();
    }

    public Collection<Integer> removeRecord(Iterable<String> record) {
        return clusterMapBuilder.removeRecord(record);
    }
}
