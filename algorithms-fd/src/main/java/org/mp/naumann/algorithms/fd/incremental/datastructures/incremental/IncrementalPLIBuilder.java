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

package org.mp.naumann.algorithms.fd.incremental.datastructures.incremental;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class IncrementalPLIBuilder {
    private final List<Integer> pliOrder;
    private List<PositionListIndex> plis;
    private final boolean isNullEqualNull;

    IncrementalPLIBuilder(boolean isNullEqualNull, List<Integer> pliOrder) {
        this.isNullEqualNull = isNullEqualNull;
        this.pliOrder = pliOrder;
    }

    private static IntArrayList concat(IntArrayList into, IntArrayList from) {
        into.addAll(from);
        return into;
    }

    /**
     * Creates the actual positionListIndices based on the clusterMaps calculated beforehand.
     * Clusters of size 1 are being discarded in the process.
     *
     * @return clustersPerAttribute,
     */
    List<PositionListIndex> fetchPositionListIndexes(List<? extends Map<Integer, IntArrayList>> clusterMaps) {
        SpeedBenchmark.begin(BenchmarkLevel.OPERATION);
        List<PositionListIndex> old = plis;
        if (old == null) {
            old = new ArrayList<>(pliOrder.size());
            for (int i = 0; i < pliOrder.size(); i++) {
                old.add(new PositionListIndex(i, isNullEqualNull, new HashMap<>()));
            }
        }
        plis = new ArrayList<>();
        int i = 0;
        for (int columnId : pliOrder) {
            Map<Integer, IntArrayList> clusters = old.get(i++).getRawClusters();
            Map<Integer, IntArrayList> newClusters = clusterMaps.get(columnId);
            newClusters.forEach((k, v) -> clusters.merge(k, v, IncrementalPLIBuilder::concat));

            plis.add(new PositionListIndex(columnId, isNullEqualNull, clusters));
        }
        return plis;
    }
}
