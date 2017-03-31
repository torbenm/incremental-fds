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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import org.mp.naumann.algorithms.benchmark.speed.Benchmark;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.incremental.Factory;
import org.mp.naumann.algorithms.fd.structures.ClusterMapBuilder;
import org.mp.naumann.algorithms.fd.utils.CollectionUtils;

class RecomputeClusterMapBuilder {

    private int numRecords = 0;
    private final List<Map<String, Collection<Integer>>> clusterMaps;
    private final Factory<Collection<Integer>> clusterFactory;

    RecomputeClusterMapBuilder(ClusterMapBuilder clusterMapBuilder,
        Factory<Collection<Integer>> clusterFactory) {
        this.clusterFactory = clusterFactory;
        List<HashMap<String, IntArrayList>> oldClusterMaps = clusterMapBuilder.getClusterMaps();
        clusterMaps = new ArrayList<>();
        for (HashMap<String, IntArrayList> oldClusterMap : oldClusterMaps) {
            Map<String, Collection<Integer>> clusterMap = new HashMap<>();
            for (Entry<String, IntArrayList> entry : oldClusterMap.entrySet()) {
                Collection<Integer> newCluster = this.clusterFactory.create();
                newCluster.addAll(entry.getValue());
                clusterMap.put(entry.getKey(), newCluster);
            }
            clusterMaps.add(clusterMap);
        }
        numRecords = clusterMapBuilder.getNumLastRecords();
    }


    List<Map<String, Collection<Integer>>> getClusterMaps() {
        return clusterMaps;
    }

    int getNumLastRecords() {
        return this.numRecords;
    }

    int addRecord(Iterable<String> record) {
        int recId = this.numRecords;
        int attributeId = 0;

        for (String value : record) {
            Map<String, Collection<Integer>> clusterMap = clusterMaps.get(attributeId);
            if (clusterMap.containsKey(value)) {
                clusterMap.get(value).add(recId);
            } else {
                Collection<Integer> newCluster = clusterFactory.create();
                newCluster.add(recId);
                clusterMap.put(value, newCluster);
            }

            attributeId++;
        }
        this.numRecords++;
        return recId;
    }

    private void logNotFoundWarning(Iterable<String> record) {
        FDLogger.log(Level.WARNING, String.format("Trying to remove %s, but there is no such record.", record.toString()));
    }

    Collection<Integer> removeRecord(Iterable<String> record) {
        Benchmark benchmark = Benchmark.start("Remove record", Benchmark.DEFAULT_LEVEL + 7);
        int attributeId = 0;
        List<Collection<Integer>> clusters = new ArrayList<>();
        for (String value : record) {
            Map<String, Collection<Integer>> clusterMap = clusterMaps.get(attributeId);
            Collection<Integer> cluster = clusterMap.get(value);
            if (cluster == null || cluster.isEmpty()) {
                logNotFoundWarning(record);
                return Collections.emptyList();
            }
            clusters.add(cluster);
            attributeId++;
        }
        benchmark.finishSubtask("Retrieve clusters");
        Set<Integer> matching = CollectionUtils.intersection(clusters);
        benchmark.finishSubtask("Intersection");
        clusters.forEach(c -> c.removeAll(matching));
        benchmark.finishSubtask("Apply");
        if (matching.isEmpty()) logNotFoundWarning(record);
        return matching;
    }

}
