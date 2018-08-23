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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.Collection;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mp.naumann.algorithms.fd.incremental.datastructures.recompute.Cluster;

class IncrementalPLIBuilder {
    private final IntList pliOrder;
    private List<MapPositionListIndex> plis;

    IncrementalPLIBuilder(IntList pliOrder) {
        this.pliOrder = pliOrder;
    }

    private static Cluster concat(Cluster into, Cluster from) {
        into.addAll(from);
        return into;
    }

    /**
     * Creates the actual positionListIndices based on the clusterMaps calculated beforehand.
     * Clusters of size 1 are being discarded in the process.
     *
     * @return clustersPerAttribute,
     */
    List<? extends PositionListIndex> fetchPositionListIndexes(List<Int2ObjectMap<Cluster>> clusterMaps) {
        List<MapPositionListIndex> old = plis;
        if (old == null) {
            old = new ArrayList<>(pliOrder.size());
            for (int i = 0; i < pliOrder.size(); i++) {
                old.add(new MapPositionListIndex(i, new Int2ObjectOpenHashMap<>()));
            }
        }
        plis = new ArrayList<>();
        int i = 0;
        for (int columnId : pliOrder) {
            Int2ObjectMap<Cluster> clusters = old.get(i++).getRawClusters();
            Int2ObjectMap<Cluster> newClusters = clusterMaps.get(columnId);
            newClusters.forEach((k, v) -> clusters.merge(k, v, IncrementalPLIBuilder::concat));

            plis.add(new MapPositionListIndex(columnId, clusters));
        }
        return plis;
    }
}
