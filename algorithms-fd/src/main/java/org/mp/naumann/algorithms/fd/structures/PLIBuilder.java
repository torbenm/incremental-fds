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

package org.mp.naumann.algorithms.fd.structures;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import org.mp.naumann.algorithms.benchmark.speed.BenchmarkLevel;
import org.mp.naumann.algorithms.benchmark.speed.SpeedBenchmark;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.database.TableInput;
import org.mp.naumann.database.data.Row;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class PLIBuilder {

    private final ClusterMapBuilder clusterMapBuilder;
    private final boolean isNullEqualNull;
    private List<Integer> pliOrder;
    private List<String> dictionary = new ArrayList<String>();


    public List<Integer> getPliOrder() {
        return pliOrder;
    }

    public List<HashMap<String, IntArrayList>> getClusterMaps() {
        return clusterMapBuilder.getClusterMaps();
    }

    public int getNumLastRecords() {
        return clusterMapBuilder.getNumLastRecords();
    }

    public PLIBuilder(int numAttributes, boolean isNullEqualNull) {
        this.clusterMapBuilder = new ClusterMapBuilder(numAttributes);
        this.isNullEqualNull = isNullEqualNull;
    }

    public void addRecords(TableInput tableInput) {
        clusterMapBuilder.addRecords(tableInput);
    }

    /**
     * Creates the actual positionListIndices based on the clusterMaps calculated beforehand.
     * Clusters of size 1 are being discarded in the process.
     *
     * @return clustersPerAttribute,
     */
    public List<PositionListIndex> fetchPositionListIndexes() {
        SpeedBenchmark.begin(BenchmarkLevel.OPERATION);
        List<HashMap<String, IntArrayList>> clusterMaps = clusterMapBuilder.getClusterMaps();
        List<PositionListIndex> clustersPerAttribute = new ArrayList<>();
        for (int columnId = 0; columnId < clusterMaps.size(); columnId++) {
            List<IntArrayList> clusters = new ArrayList<>();
            HashMap<String, IntArrayList> clusterMap = clusterMaps.get(columnId);

            if (!isNullEqualNull)
                clusterMap.remove(null);

            for (IntArrayList cluster : clusterMap.values())
                if (cluster.size() > 1)
                    clusters.add(cluster);

            clustersPerAttribute.add(new PositionListIndex(columnId, clusters));
        }
        // Sort plis by number of clusters: For searching in the covers and for
        // validation, it is good to have attributes with few non-unique values
        // and many clusters left in the prefix tree
        FDLogger.log(Level.FINER, "Sorting plis by number of clusters ...");
        if (pliOrder == null) {
            pliOrder = clustersPerAttribute.stream().sorted((o1, o2) -> {
                int numClustersInO1 = numClusters(o1);
                int numClustersInO2 = numClusters(o2);
                return numClustersInO2 - numClustersInO1;
            }).map(PositionListIndex::getAttribute).collect(Collectors.toList());
        }
        List<PositionListIndex> plis = new ArrayList<>(clustersPerAttribute.size());
        for (int attributeId : pliOrder) {
            plis.add(clustersPerAttribute.get(attributeId));
        }
        SpeedBenchmark.lap(BenchmarkLevel.OPERATION, "Sorted plis by cluster");
        return plis;

    }

    private int numClusters(PositionListIndex idx) {
        return getNumLastRecords() - idx.getNumNonUniqueValues() + idx.getClusters().size();
    }

    public void addRecords(ObjectArrayList<Row> records) {
        clusterMapBuilder.addRecords(records);
    }

    public int addRecord(Iterable<String> record) {
        return clusterMapBuilder.addRecord(record);
    }
    public Set<Integer> removeRecord(Iterable<String> record) {
        return clusterMapBuilder.removeRecord(record);
    }

    public void removeRecords(Iterable<String> record, Set<Integer> recordIds) {
        clusterMapBuilder.removeRecords(record, recordIds);
    }
}
