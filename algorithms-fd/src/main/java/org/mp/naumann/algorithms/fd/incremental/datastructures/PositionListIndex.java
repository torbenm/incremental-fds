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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.hyfd.PLIBuilder;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.structures.ClusterIdentifier;
import org.mp.naumann.algorithms.fd.structures.ClusterIdentifierWithRecord;
import org.mp.naumann.algorithms.fd.structures.IPositionListIndex;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.utils.CollectionUtils;
import org.mp.naumann.algorithms.fd.utils.PliUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Position list indices (or stripped partitions) are an index structure that
 * stores the positions of equal values in a nested list. A column with the
 * values a, a, b, c, b, c transfers to the position list index ((0, 1), (2, 4),
 * (3, 5)). Clusters of size 1 are discarded. A position list index should be
 * created using the {@link PLIBuilder}.
 */
public abstract class PositionListIndex implements IPositionListIndex {

    private final int attribute;
    private List<IntArrayList> clustersWithNewRecords = null;
    private Collection<Integer> newRecords = null;

    @Override
    public int getAttribute() {
        return this.attribute;
    }

    protected PositionListIndex(int attribute) {
        this.attribute = attribute;
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
        if (numRecords <= 1)
            return true;
        return (getClusters().size() == 1) && (getClusters().iterator().next().size() == numRecords);
    }


    public boolean refines(CompressedRecords compressedRecords, int rhsAttr) {
        for (IntArrayList cluster : getClustersToCheck())
            if (!this.probe(compressedRecords, rhsAttr, cluster))
                return false;
        return true;
    }

    private boolean probe(CompressedRecords compressedRecords, int rhsAttr, IntArrayList cluster) {
        int rhsClusterId = compressedRecords.get(cluster.getInt(0))[rhsAttr];

        // If otherClusterId < 0, then this cluster must point into more than one other clusters
        if (rhsClusterId == PliUtils.UNIQUE_VALUE)
            return false;

        // Check if all records of this cluster point into the same other cluster
        for (int recordId : cluster)
            if (compressedRecords.get(recordId)[rhsAttr] != rhsClusterId)
                return false;

        return true;
    }


    public OpenBitSet refines(CompressedRecords compressedRecords, OpenBitSet lhs, OpenBitSet rhs, List<IntegerPair> comparisonSuggestions) {
        int rhsSize = (int) rhs.cardinality();
        int lhsSize = (int) lhs.cardinality();

        // Returns the rhs attributes that are refined by the lhs
        OpenBitSet refinedRhs = rhs.clone();

        // TODO: Check if it is technically possible that this fd holds, i.e., if A1 has 2 clusters of size 10 and A2 has 2 clusters of size 10, then the intersection can have at most 4 clusters of size 5 (see join cardinality estimation)

        int[] rhsAttrId2Index = new int[compressedRecords.get(0).length];
        int[] rhsAttrIndex2Id = new int[rhsSize];
        int index = 0;
        for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
            rhsAttrId2Index[rhsAttr] = index;
            rhsAttrIndex2Id[index] = rhsAttr;
            index++;
        }

        boolean useInnerClusterPruning = useInnerClusterPruning();
        for (IntArrayList cluster : getClustersToCheck()) {
            Object2ObjectOpenHashMap<ClusterIdentifier, ClusterIdentifierWithRecord> subClusters = new Object2ObjectOpenHashMap<>(cluster.size());
            ObjectOpenHashSet<ClusterIdentifier> haveOldRecord = null;
            if (useInnerClusterPruning) {
                haveOldRecord = new ObjectOpenHashSet<>(cluster.size());
            }
            for (int recordId : cluster) {
                ClusterIdentifier subClusterIdentifier = this.buildClusterIdentifier(lhs, lhsSize, compressedRecords.get(recordId));
                if (subClusterIdentifier == null)
                    continue;

                boolean isOldRecord = false;
                if (useInnerClusterPruning) {
                    isOldRecord = isOldRecord(recordId);
                }
                if (subClusters.containsKey(subClusterIdentifier)) {
                    if (useInnerClusterPruning && isOldRecord) {
                        if (haveOldRecord.contains(subClusterIdentifier)) {
                            continue;
                        } else {
                            haveOldRecord.add(subClusterIdentifier);
                        }
                    }
                    ClusterIdentifierWithRecord rhsClusters = subClusters.get(subClusterIdentifier);

                    for (int rhsAttr = refinedRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = refinedRhs.nextSetBit(rhsAttr + 1)) {
                        int rhsCluster = compressedRecords.get(recordId)[rhsAttr];
                        if ((rhsCluster == PliUtils.UNIQUE_VALUE) || (rhsCluster != rhsClusters.get(rhsAttrId2Index[rhsAttr]))) {
                            comparisonSuggestions.add(new IntegerPair(recordId, rhsClusters.getRecord()));

                            refinedRhs.clear(rhsAttr);
                            if (refinedRhs.isEmpty())
                                return refinedRhs;
                        }
                    }
                } else {
                    int[] rhsClusters = new int[rhsSize];
                    for (int rhsAttr = 0; rhsAttr < rhsSize; rhsAttr++)
                        rhsClusters[rhsAttr] = compressedRecords.get(recordId)[rhsAttrIndex2Id[rhsAttr]];
                    subClusters.put(subClusterIdentifier, new ClusterIdentifierWithRecord(rhsClusters, recordId));
                    if (useInnerClusterPruning && isOldRecord) {
                        haveOldRecord.add(subClusterIdentifier);
                    }
                }
            }
        }
        return refinedRhs;
    }

    private boolean isOldRecord(int recordId) {
        return !newRecords.contains(recordId);
    }

    private boolean useInnerClusterPruning() {
        return newRecords != null;
    }

    public void setNewRecords(Collection<Integer> newRecords) {
        this.newRecords = newRecords;
    }

    public void setClustersWithNewRecords(Collection<Integer> newRecords, CompressedRecords compressedRecords, int attribute) {
        Set<Integer> clusterIds = new HashSet<>();
        for (int id : newRecords) {
            int clusterId = compressedRecords.get(id)[attribute];
            if (clusterId != PliUtils.UNIQUE_VALUE) {
                clusterIds.add(clusterId);
            }
        }
        clustersWithNewRecords = clusterIds.stream().map(this::getCluster).collect(Collectors.toList());
    }

    protected abstract IntArrayList getCluster(int index);

    private Collection<IntArrayList> getClustersToCheck() {
        Collection<IntArrayList> toCheck = clustersWithNewRecords == null ? getClusters() : clustersWithNewRecords;
        return toCheck.stream().filter(c -> c.size() > 1).collect(Collectors.toList());
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

        List<IntOpenHashSet> setCluster = this.convertClustersToSets(getClusters());

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
        if (getClusters() == null) {
            if (other.getClusters() != null) {
                return false;
            }
        } else {
            List<IntOpenHashSet> setCluster = this.convertClustersToSets(getClusters());
            List<IntOpenHashSet> otherSetCluster = this.convertClustersToSets(getClusters());

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
        for (IntArrayList cluster : getClusters()) {
            builder.append("{");
            builder.append(CollectionUtils.concat(cluster, ","));
            builder.append("} ");
        }
        builder.append("}");
        return builder.toString();
    }

    private List<IntOpenHashSet> convertClustersToSets(Collection<IntArrayList> listCluster) {
        List<IntOpenHashSet> setClusters = new LinkedList<>();
        for (IntArrayList cluster : listCluster) {
            setClusters.add(new IntOpenHashSet(cluster));
        }

        return setClusters;
    }
}
