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
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntLists;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mp.naumann.algorithms.fd.incremental.Factory;
import org.mp.naumann.algorithms.fd.incremental.datastructures.recompute.Cluster;
import org.mp.naumann.algorithms.fd.utils.CollectionUtils;

class IncrementalClusterMapBuilder {

    private int nextRecordId;
    private final List<Int2ObjectMap<Cluster>> clusterMaps;
    private final Dictionary<String> dictionary;
    private final Factory<Cluster> clusterFactory;

    IncrementalClusterMapBuilder(int numAttributes, int nextRecordId, Dictionary<String> dictionary,
        Factory<Cluster> clusterFactory) {
        this.dictionary = dictionary;
        this.nextRecordId = nextRecordId;
        this.clusterMaps = new ArrayList<>(numAttributes);
        this.clusterFactory = clusterFactory;
        for (int i = 0; i < numAttributes; i++) {
            clusterMaps.add(new Int2ObjectOpenHashMap<>());
        }
    }

    public int getNextRecordId() {
        return nextRecordId;
    }

    List<Int2ObjectMap<Cluster>> getClusterMaps() {
        return clusterMaps;
    }

    int addRecord(Iterable<String> record) {
        int recId = this.nextRecordId++;
        int attributeId = 0;
        for (String value : record) {
            Int2ObjectMap<Cluster> clusterMap = clusterMaps.get(attributeId);
            int dictValue = dictionary.getOrAdd(value);
            if (clusterMap.containsKey(dictValue)) {
                clusterMap.get(dictValue).add(recId);
            } else {
                Cluster newCluster = clusterFactory.create();
                newCluster.add(recId);
                clusterMap.put(dictValue, newCluster);
            }

            attributeId++;
        }
        return recId;
    }

    IntCollection removeRecord(Iterable<String> record) {
        int attributeId = 0;
        List<Cluster> clusters = new ArrayList<>();
        for (String value : record) {
            Int2ObjectMap<Cluster> clusterMap = clusterMaps.get(attributeId);
            int dictValue = dictionary.getOrAdd(value);
            Cluster cluster = clusterMap.get(dictValue);
            if (cluster == null || cluster.isEmpty()) {
                return IntLists.EMPTY_LIST;
            }
            clusters.add(cluster);
            attributeId++;
        }
        IntSet matching = CollectionUtils.intersection(clusters);
        clusters.forEach(c -> c.removeAll(matching));
        return matching;
    }

    void flush() {
        clusterMaps.forEach(Map::clear);
    }
}
