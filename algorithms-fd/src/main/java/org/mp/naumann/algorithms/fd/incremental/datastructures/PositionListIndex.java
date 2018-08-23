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

package org.mp.naumann.algorithms.fd.incremental.datastructures;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap.Entry;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.datastructures.recompute.Cluster;
import org.mp.naumann.algorithms.fd.structures.ClusterIdentifier;
import org.mp.naumann.algorithms.fd.structures.ClusterIdentifierWithRecord;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.PLIBuilder;
import org.mp.naumann.algorithms.fd.utils.CollectionUtils;
import org.mp.naumann.algorithms.fd.utils.PliUtils;

/**
 * Position list indices (or stripped partitions) are an index structure that
 * stores the positions of equal values in a nested list. A column with the
 * values a, a, b, c, b, c transfers to the position list index ((0, 1), (2, 4),
 * (3, 5)). Clusters of size 1 are discarded. A position list index should be
 * created using the {@link PLIBuilder}.
 */
public abstract class PositionListIndex {

    private final int attribute;
    private List<Cluster> clustersWithNewRecords = null;
    private Int2ObjectMap<IntSet> otherClustersWithNewRecords;
    private Integer newRecordBoundary;

    protected PositionListIndex(int attribute) {
        this.attribute = attribute;
    }

    public abstract Iterable<Entry<Cluster>> getClustersWithKey();
    public abstract Collection<Cluster> getClusters();
    public abstract Cluster getCluster(int index);

    public int getAttribute() {
        return this.attribute;
    }

    /**
     * Returns the number of non unary clusters.
     *
     * @return the number of clusters in the {@link PositionListIndex}
     */
    public long size() {
        return getClusters().size();
    }

    public boolean isConstant(int numRecords) {
        if (numRecords <= 1) {
            return true;
        }
        return (size() == 1) && (getClusters().iterator().next().size() == numRecords);
    }


    public boolean refines(CompressedRecords compressedRecords, int rhsAttr, boolean topDown) {
        Iterator<Cluster> it = getClustersToCheck(topDown);
        while (it.hasNext()) {
            Cluster cluster = it.next();
            if (!this.probe(compressedRecords, rhsAttr, cluster)) {
                return false;
            }
        }
        return true;
    }

    private boolean probe(CompressedRecords compressedRecords, int rhsAttr, Cluster cluster) {
        int rhsClusterId = compressedRecords.get(cluster.iterator().next())[rhsAttr];

        // If otherClusterId < 0, then this cluster must point into more than one other clusters
        if (rhsClusterId == PliUtils.UNIQUE_VALUE) {
            return false;
        }

        // Check if all records of this cluster point into the same other cluster
        for (int recordId : cluster) {
            if (compressedRecords.get(recordId)[rhsAttr] != rhsClusterId) {
                return false;
            }
        }

        return true;
    }


    public OpenBitSet refines(CompressedRecords compressedRecords, OpenBitSet lhs, OpenBitSet rhs, List<IntegerPair> comparisonSuggestions, boolean topDown) {
        int rhsSize = (int) rhs.cardinality();
        int lhsSize = (int) lhs.cardinality();

        // Returns the rhs attributes that are refined by the lhs
        OpenBitSet refinedRhs = rhs.clone();

        // TODO: Check if it is technically possible that this fd holds, i.e., if A1 has 2 clusters of size 10 and A2 has 2 clusters of size 10, then the intersection can have at most 4 clusters of size 5 (see join cardinality estimation)

        int[] rhsAttrId2Index = new int[compressedRecords.getNumAttributes()];
        int[] rhsAttrIndex2Id = new int[rhsSize];
        int index = 0;
        for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
            rhsAttrId2Index[rhsAttr] = index;
            rhsAttrIndex2Id[index] = rhsAttr;
            index++;
        }

        boolean useInnerClusterPruning = useInnerClusterPruning(topDown);
        Iterator<Cluster> it = getClustersToCheck(topDown);
        while (it.hasNext()) {
            Cluster cluster = it.next();
            Object2ObjectOpenHashMap<ClusterIdentifier, ClusterIdentifierWithRecord> subClusters = new Object2ObjectOpenHashMap<>(cluster.size());
            for (int recordId : cluster) {
                ClusterIdentifier subClusterIdentifier = this.buildClusterIdentifier(lhs, lhsSize, compressedRecords.get(recordId), topDown);
                if (subClusterIdentifier == null) {
                    continue;
                }

                boolean isOldRecord = false;
                if (useInnerClusterPruning) {
                    isOldRecord = isOldRecord(recordId);
                }
                if (subClusters.containsKey(subClusterIdentifier)) {
                    if (useInnerClusterPruning && isOldRecord) {
                        continue;
                    }
                    ClusterIdentifierWithRecord rhsClusters = subClusters.get(subClusterIdentifier);

                    for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
                        int rhsCluster = compressedRecords.get(recordId)[rhsAttr];
                        if ((rhsCluster == PliUtils.UNIQUE_VALUE) || (rhsCluster != rhsClusters.get(rhsAttrId2Index[rhsAttr]))) {
                            comparisonSuggestions.add(new IntegerPair(recordId, rhsClusters.getRecord()));

                            refinedRhs.fastClear(rhsAttr);
                            if (refinedRhs.isEmpty()) {
                                return refinedRhs;
                            }
                        }
                    }
                } else {
                    int[] rhsClusters = new int[rhsSize];
                    for (int rhsAttr = 0; rhsAttr < rhsSize; rhsAttr++) {
                        rhsClusters[rhsAttr] = compressedRecords.get(recordId)[rhsAttrIndex2Id[rhsAttr]];
                    }
                    subClusters.put(subClusterIdentifier, new ClusterIdentifierWithRecord(rhsClusters, recordId));
                }
            }
        }
        return refinedRhs;
    }

    private boolean isOldRecord(int recordId) {
        return recordId < newRecordBoundary;
    }

    private boolean useInnerClusterPruning(boolean topDown) {
        return topDown && newRecordBoundary != null;
    }

    public void setNewRecordBoundary(Integer newRecordBoundary) {
        this.newRecordBoundary = newRecordBoundary;
    }

    public void setOtherClustersWithNewRecords(Int2ObjectMap<IntSet> otherClustersWithNewRecords) {
        this.otherClustersWithNewRecords = otherClustersWithNewRecords;
    }

    public void setClustersWithNewRecords(IntCollection clusterIds) {
        clustersWithNewRecords = clusterIds.stream().map(this::getCluster).collect(Collectors.toList());
    }

    public Iterator<Cluster> getClustersToCheck(boolean topDown) {
        final Collection<Cluster> toCheck;
        if (topDown) {
            toCheck = clustersWithNewRecords == null ? getClusters() : clustersWithNewRecords;
        } else {
            toCheck = getClusters();
        }
        return toCheck.stream().filter(c -> c.size() > 1).iterator();
    }

    private ClusterIdentifier buildClusterIdentifier(OpenBitSet lhs, int lhsSize, int[] record,
                                                     boolean topDown) {
        int[] cluster = new int[lhsSize];

        int index = 0;
        for (int lhsAttr = lhs.nextSetBit(0); lhsAttr >= 0; lhsAttr = lhs.nextSetBit(lhsAttr + 1)) {
            int clusterId = record[lhsAttr];

            if (clusterId < 0) {
                return null;
            }

            if (topDown && otherClustersWithNewRecords != null && !otherClustersWithNewRecords.get(lhsAttr).contains(clusterId)) {
                return null;
            }

            cluster[index] = clusterId;
            index++;
        }

        return new ClusterIdentifier(cluster);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;

        List<IntOpenHashSet> setCluster = this.convertClustersToSets();

        setCluster.sort(Comparator.comparingInt(IntOpenHashSet::hashCode));
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
        if (getClusters() == null) {
            if (other.getClusters() != null) {
                return false;
            }
        } else {
            List<IntOpenHashSet> setCluster = this.convertClustersToSets();
            List<IntOpenHashSet> otherSetCluster = other.convertClustersToSets();

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
        for (Cluster cluster : getClusters()) {
            builder.append("{");
            builder.append(CollectionUtils.concat(cluster.toCollection(), ","));
            builder.append("} ");
        }
        builder.append("}");
        return builder.toString();
    }

    private List<IntOpenHashSet> convertClustersToSets() {
        List<IntOpenHashSet> setClusters = new LinkedList<>();
        for (Cluster cluster : getClusters()) {
            setClusters.add(new IntOpenHashSet(cluster.toCollection()));
        }

        return setClusters;
    }
}
