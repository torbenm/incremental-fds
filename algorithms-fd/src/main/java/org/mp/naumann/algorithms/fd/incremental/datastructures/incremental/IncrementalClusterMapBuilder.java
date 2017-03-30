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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mp.naumann.algorithms.fd.incremental.Factory;
import org.mp.naumann.algorithms.fd.utils.CollectionUtils;

class IncrementalClusterMapBuilder {

    private int nextRecordId;
    private final List<Map<Integer, Collection<Integer>>> clusterMaps;
    private final Dictionary<String> dictionary;
    private final Factory<Collection<Integer>> clusterFactory;

    IncrementalClusterMapBuilder(int numAttributes, int nextRecordId, Dictionary<String> dictionary,
        Factory<Collection<Integer>> clusterFactory) {
        this.dictionary = dictionary;
        this.nextRecordId = nextRecordId;
        this.clusterMaps = new ArrayList<>(numAttributes);
        this.clusterFactory = clusterFactory;
        for (int i = 0; i < numAttributes; i++) {
            clusterMaps.add(new HashMap<>());
        }
    }

    List<Map<Integer, Collection<Integer>>> getClusterMaps() {
        return clusterMaps;
    }

    int addRecord(Iterable<String> record) {
        int recId = this.nextRecordId++;
        int attributeId = 0;
        for (String value : record) {
            Map<Integer, Collection<Integer>> clusterMap = clusterMaps.get(attributeId);
            int dictValue = dictionary.getOrAdd(value);
            if (clusterMap.containsKey(dictValue)) {
                clusterMap.get(dictValue).add(recId);
            } else {
                Collection<Integer> newCluster = clusterFactory.create();
                newCluster.add(recId);
                clusterMap.put(dictValue, newCluster);
            }

            attributeId++;
        }
        return recId;
    }

    Collection<Integer> removeRecord(Iterable<String> record) {
        int attributeId = 0;
        List<Collection<Integer>> clusters = new ArrayList<>();
        for (String value : record) {
            Map<Integer, Collection<Integer>> clusterMap = clusterMaps.get(attributeId);
            int dictValue = dictionary.getOrAdd(value);
            Collection<Integer> cluster = clusterMap.get(dictValue);
            if (cluster == null || cluster.isEmpty()) {
                return Collections.emptyList();
            }
            clusters.add(cluster);
            attributeId++;
        }
        Set<Integer> matching = CollectionUtils.intersection(clusters);
        clusters.forEach(c -> c.removeAll(matching));
        return matching;
    }

    void flush() {
        clusterMaps.forEach(Map::clear);
    }
}
