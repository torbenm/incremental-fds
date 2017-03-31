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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.stream.Collectors;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.hyfd.PositionListIndex;
import org.mp.naumann.database.TableInput;

public class PLIBuilder {

    private final ClusterMapBuilder clusterMapBuilder;
    private final boolean isNullEqualNull;

    public PLIBuilder(int numAttributes, boolean isNullEqualNull) {
        this.clusterMapBuilder = new ClusterMapBuilder(numAttributes);
        this.isNullEqualNull = isNullEqualNull;
    }

    public List<HashMap<String, IntArrayList>> getClusterMaps() {
        return clusterMapBuilder.getClusterMaps();
    }

    public int getNumLastRecords() {
        return clusterMapBuilder.getNumLastRecords();
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
        clustersPerAttribute.sort((o1, o2) -> {
            int numClustersInO1 = numClusters(o1);
            int numClustersInO2 = numClusters(o2);
            return numClustersInO2 - numClustersInO1;
        });

        return clustersPerAttribute;
    }

    private int numClusters(PositionListIndex idx) {
        return getNumLastRecords() - idx.getNumNonUniqueValues() + idx.getClusters().size();
    }

    public void addRecords(Collection<? extends Iterable<String>> records) {
        clusterMapBuilder.addRecords(records);
    }

    public boolean isNullEqualNull() {
        return isNullEqualNull;
    }

    public ClusterMapBuilder getClusterMapBuilder() {
        return clusterMapBuilder;
    }

    public List<Integer> getPliOrder() {
        return fetchPositionListIndexes().stream().map(PositionListIndex::getAttribute).collect(Collectors.toList());
    }

}
