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

package org.mp.naumann.algorithms.fd.hyfd;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.ClusterIdentifier;
import org.mp.naumann.algorithms.fd.structures.ClusterIdentifierWithRecord;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.PLIBuilder;
import org.mp.naumann.algorithms.fd.utils.CollectionUtils;

/**
 * Position list indices (or stripped partitions) are an index structure that
 * stores the positions of equal values in a nested list. A column with the
 * values a, a, b, c, b, c transfers to the position list index ((0, 1), (2, 4),
 * (3, 5)). Clusters of size 1 are discarded. A position list index should be
 * created using the {@link PLIBuilder}.
 */
public class PositionListIndex {

    private final int attribute;
    private final List<IntArrayList> clusters;
    private final int numNonUniqueValues;

    public PositionListIndex(int attribute, List<IntArrayList> clusters) {
        this.attribute = attribute;
        this.clusters = clusters;
        this.numNonUniqueValues = this.countNonUniqueValuesIn(clusters);
    }

    public int getAttribute() {
        return this.attribute;
    }

    public List<IntArrayList> getClusters() {
        return this.clusters;
    }

    public int getNumNonUniqueValues() {
        return this.numNonUniqueValues;
    }

    private int countNonUniqueValuesIn(List<IntArrayList> clusters) {
        int numNonUniqueValues = 0;
        for (IntArrayList cluster : clusters)
            numNonUniqueValues += cluster.size();
        return numNonUniqueValues;
    }

    public boolean isConstant(int numRecords) {
        if (numRecords <= 1)
            return true;
        return (this.clusters.size() == 1) && (this.clusters.get(0).size() == numRecords);
    }


    public boolean refines(int[][] compressedRecords, int rhsAttr) {
        for (IntArrayList cluster : this.clusters) {
            if (!this.probe(compressedRecords, rhsAttr, cluster))
                return false;
        }
        return true;
    }

    private boolean probe(int[][] compressedRecords, int rhsAttr, IntArrayList cluster) {
        if (cluster.size() == 0) return false;

        int rhsClusterId = compressedRecords[cluster.getInt(0)][rhsAttr];

        // If otherClusterId < 0, then this cluster must point into more than one other clusters
        if (rhsClusterId == -1)
            return false;

        // Check if all records of this cluster point into the same other cluster
        for (int recordId : cluster)
            if (compressedRecords[recordId][rhsAttr] != rhsClusterId)
                return false;


        return true;
    }


    public OpenBitSet refines(int[][] compressedRecords, OpenBitSet lhs, OpenBitSet rhs, List<IntegerPair> comparisonSuggestions) {
        int rhsSize = (int) rhs.cardinality();
        int lhsSize = (int) lhs.cardinality();

        // Returns the rhs attributes that are refined by the lhs
        OpenBitSet refinedRhs = rhs.clone();

        // TODO: Check if it is technically possible that this fd holds, i.e.,
        // if A1 has 2 clusters of size 10 and A2 has 2 clusters of size 10,
        // then the intersection can have at most 4 clusters of size 5 (see join cardinality estimation)

        int[] rhsAttrId2Index = new int[compressedRecords[0].length];
        int[] rhsAttrIndex2Id = new int[rhsSize];
        int index = 0;
        for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
            rhsAttrId2Index[rhsAttr] = index;
            rhsAttrIndex2Id[index] = rhsAttr;
            index++;
        }

        for (IntArrayList cluster : this.clusters) {
            Object2ObjectOpenHashMap<ClusterIdentifier, ClusterIdentifierWithRecord> subClusters = new Object2ObjectOpenHashMap<>(cluster.size());
            for (int recordId : cluster) {
                ClusterIdentifier subClusterIdentifier = this.buildClusterIdentifier(lhs, lhsSize, compressedRecords[recordId]);
                if (subClusterIdentifier == null)
                    continue;

                if (subClusters.containsKey(subClusterIdentifier)) {
                    ClusterIdentifierWithRecord rhsClusters = subClusters.get(subClusterIdentifier);

                    for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
                        int rhsCluster = compressedRecords[recordId][rhsAttr];
                        if ((rhsCluster == -1) || (rhsCluster != rhsClusters.get(rhsAttrId2Index[rhsAttr]))) {
                            comparisonSuggestions.add(new IntegerPair(recordId, rhsClusters.getRecord()));

                            refinedRhs.clear(rhsAttr);
                            if (refinedRhs.isEmpty())
                                return refinedRhs;
                        }
                    }
                } else {
                    int[] rhsClusters = new int[rhsSize];
                    for (int rhsAttr = 0; rhsAttr < rhsSize; rhsAttr++)
                        rhsClusters[rhsAttr] = compressedRecords[recordId][rhsAttrIndex2Id[rhsAttr]];
                    subClusters.put(subClusterIdentifier, new ClusterIdentifierWithRecord(rhsClusters, recordId));
                }
            }
        }
        return refinedRhs;
    }

    private ClusterIdentifier buildClusterIdentifier(OpenBitSet lhs, int lhsSize, int[] record) {
        int[] cluster = new int[lhsSize];
        int index = 0;
        for (int lhsAttr = lhs.nextSetBit(0); lhsAttr >= 0; lhsAttr = lhs.nextSetBit(lhsAttr + 1)) {
            int clusterId = record[lhsAttr];

            if (clusterId < 0)
                return null;

            cluster[index] = clusterId;
            index++;
        }

        return new ClusterIdentifier(cluster);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;

        List<IntOpenHashSet> setCluster = this.convertClustersToSets(this.clusters);

        Collections.sort(setCluster, new Comparator<IntSet>() {
            @Override
            public int compare(IntSet o1, IntSet o2) {
                return o1.hashCode() - o2.hashCode();
            }
        });
        result = prime * result + (setCluster.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        PositionListIndex other = (PositionListIndex) obj;
        if (this.clusters == null) {
            if (other.clusters != null) {
                return false;
            }
        } else {
            List<IntOpenHashSet> setCluster = this.convertClustersToSets(this.clusters);
            List<IntOpenHashSet> otherSetCluster = this.convertClustersToSets(other.clusters);

            for (IntOpenHashSet cluster : setCluster) {
                if (!otherSetCluster.contains(cluster)) {
                    return false;
                }
            }
            for (IntOpenHashSet cluster : otherSetCluster) {
                if (!setCluster.contains(cluster)) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("{ ");
        for (IntArrayList cluster : this.clusters) {
            builder.append("{");
            builder.append(CollectionUtils.concat(cluster, ","));
            builder.append("} ");
        }
        builder.append("}");
        return builder.toString();
    }

    private List<IntOpenHashSet> convertClustersToSets(List<IntArrayList> listCluster) {
        List<IntOpenHashSet> setClusters = new LinkedList<>();
        for (IntArrayList cluster : listCluster) {
            setClusters.add(new IntOpenHashSet(cluster));
        }

        return setClusters;
    }
}
