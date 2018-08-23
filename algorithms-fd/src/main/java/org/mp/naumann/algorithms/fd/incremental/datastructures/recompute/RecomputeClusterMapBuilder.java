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
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntCollections;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import it.unimi.dsi.fastutil.ints.IntSet;
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
    private final List<Map<String, Cluster>> clusterMaps;
    private final Factory<Cluster> clusterFactory;

    RecomputeClusterMapBuilder(ClusterMapBuilder clusterMapBuilder,
        Factory<Cluster> clusterFactory) {
        this.clusterFactory = clusterFactory;
        List<Map<String, IntList>> oldClusterMaps = clusterMapBuilder.getClusterMaps();
        clusterMaps = new ArrayList<>();
        for (Map<String, IntList> oldClusterMap : oldClusterMaps) {
            Map<String, Cluster> clusterMap = new HashMap<>();
            for (Entry<String, IntList> entry : oldClusterMap.entrySet()) {
                Cluster newCluster = this.clusterFactory.create();
                newCluster.addAll(entry.getValue());
                clusterMap.put(entry.getKey(), newCluster);
            }
            clusterMaps.add(clusterMap);
        }
        numRecords = clusterMapBuilder.getNumLastRecords();
    }


    List<Map<String, Cluster>> getClusterMaps() {
        return clusterMaps;
    }

    int getNumLastRecords() {
        return this.numRecords;
    }

    int addRecord(Iterable<String> record) {
        int recId = this.numRecords++;
        int attributeId = 0;

        for (String value : record) {
            Map<String, Cluster> clusterMap = clusterMaps.get(attributeId);
            if (clusterMap.containsKey(value)) {
                clusterMap.get(value).add(recId);
            } else {
                Cluster newCluster = clusterFactory.create();
                newCluster.add(recId);
                clusterMap.put(value, newCluster);
            }

            attributeId++;
        }
        return recId;
    }

    private void logNotFoundWarning(Iterable<String> record) {
        FDLogger.log(Level.WARNING, String.format("Trying to remove %s, but there is no such record.", record.toString()));
    }

    IntCollection removeRecord(Iterable<String> record) {
        Benchmark benchmark = Benchmark.start("Remove record", Benchmark.DEFAULT_LEVEL + 7);
        int attributeId = 0;
        List<Cluster> clusters = new ArrayList<>();
        for (String value : record) {
            Map<String, Cluster> clusterMap = clusterMaps.get(attributeId);
            Cluster cluster = clusterMap.get(value);
            if (cluster == null || cluster.isEmpty()) {
                logNotFoundWarning(record);
                return IntLists.EMPTY_LIST;
            }
            clusters.add(cluster);
            attributeId++;
        }
        benchmark.finishSubtask("Retrieve clusters");
        IntSet matching = CollectionUtils.intersection(clusters);
        benchmark.finishSubtask("Intersection");
        clusters.forEach(c -> c.removeAll(matching));
        benchmark.finishSubtask("Apply");
        if (matching.isEmpty()) logNotFoundWarning(record);
        return matching;
    }

}
