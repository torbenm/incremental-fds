package org.mp.naumann.algorithms.fd.hyfd;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.logging.Level;

class Sampler {

    private final Matcher matcher;
    private FDSet negCover;
    private FDTree posCover;
    private int[][] compressedRecords;
    private List<PositionListIndex> plis;
    private float efficiencyThreshold;
    private List<AttributeRepresentant> attributeRepresentants = null;
    private MemoryGuardian memoryGuardian;

    public Sampler(FDSet negCover, FDTree posCover, int[][] compressedRecords, List<PositionListIndex> plis, float efficiencyThreshold, MemoryGuardian memoryGuardian, Matcher matcher) {
        this.negCover = negCover;
        this.posCover = posCover;
        this.compressedRecords = compressedRecords;
        this.plis = plis;
        this.efficiencyThreshold = efficiencyThreshold;
        this.memoryGuardian = memoryGuardian;
        this.matcher = matcher;
    }

    public FDList enrichNegativeCover(List<IntegerPair> comparisonSuggestions) {
        int numAttributes = this.compressedRecords[0].length;

        FDLogger.log(Level.FINEST, "Investigating comparison suggestions ... ");
        FDList newNonFds = new FDList(numAttributes, this.negCover.getMaxDepth());
        OpenBitSet equalAttrs = new OpenBitSet(this.posCover.getNumAttributes());
        for (IntegerPair comparisonSuggestion : comparisonSuggestions) {

            this.matcher.match(equalAttrs, comparisonSuggestion.a(), comparisonSuggestion.b());

            if (!this.negCover.contains(equalAttrs)) {
                OpenBitSet equalAttrsCopy = equalAttrs.clone();
                this.negCover.add(equalAttrsCopy);
                newNonFds.add(equalAttrsCopy);

                this.memoryGuardian.memoryChanged(1);
                this.memoryGuardian.match(this.negCover, this.posCover, newNonFds);
            }
        }

        if (this.attributeRepresentants == null) { // if this is the first call of this method
            FDLogger.log(Level.FINEST, "Sorting clusters ...");
            long time = System.currentTimeMillis();
            ClusterComparator comparator = new ClusterComparator(this.compressedRecords, this.compressedRecords[0].length - 1, 1);
            for (PositionListIndex pli : this.plis) {
                for (IntArrayList cluster : pli.getClusters()) {
                    Collections.sort(cluster, comparator);
                }
                comparator.incrementActiveKey();
            }
            FDLogger.log(Level.FINEST, "(" + (System.currentTimeMillis() - time) + "ms)");

            FDLogger.log(Level.FINEST, "Running initial windows ...");
            time = System.currentTimeMillis();
            this.attributeRepresentants = new ArrayList<>(numAttributes);
            float efficiencyFactor = (int) Math.ceil(1 / this.efficiencyThreshold);
            for (int i = 0; i < numAttributes; i++) {
                AttributeRepresentant attributeRepresentant = new AttributeRepresentant(this.plis.get(i).getClusters(), efficiencyFactor, this.negCover, this.posCover, this, this.memoryGuardian);
                attributeRepresentant.runNext(newNonFds, this.compressedRecords);
                if (attributeRepresentant.getEfficiency() != 0)
                    this.attributeRepresentants.add(attributeRepresentant);
            }
            FDLogger.log(Level.FINEST, "(" + (System.currentTimeMillis() - time) + "ms)");
        } else {
            // Lower the efficiency factor for this round
            for (AttributeRepresentant attributeRepresentant : this.attributeRepresentants) {
                attributeRepresentant.setEfficiencyFactor(attributeRepresentant.getEfficiencyFactor() * 2); // TODO: find a more clever way to increase the efficiency expectation
            }
        }

        FDLogger.log(Level.FINEST, "Moving window over clusters ... ");
        PriorityQueue<AttributeRepresentant> queue = new PriorityQueue<>(this.attributeRepresentants);
        while (!queue.isEmpty()) {
            AttributeRepresentant attributeRepresentant = queue.remove();
            if (!attributeRepresentant.runNext(newNonFds, this.compressedRecords))
                continue;

            if (attributeRepresentant.getEfficiency() != 0)
                queue.add(attributeRepresentant);
        }
        return newNonFds;
    }

    private class ClusterComparator implements Comparator<Integer> {

        private int[][] sortKeys;
        private int activeKey1;
        private int activeKey2;

        public ClusterComparator(int[][] sortKeys, int activeKey1, int activeKey2) {
            super();
            this.sortKeys = sortKeys;
            this.activeKey1 = activeKey1;
            this.activeKey2 = activeKey2;
        }

        public void incrementActiveKey() {
            this.activeKey1 = this.increment(this.activeKey1);
            this.activeKey2 = this.increment(this.activeKey2);
        }

        @Override
        public int compare(Integer o1, Integer o2) {
            // Previous -> Next
            int value1 = this.sortKeys[o1][this.activeKey1];
            int value2 = this.sortKeys[o2][this.activeKey1];
            int result = value2 - value1;
            if (result == 0) {
                value1 = this.sortKeys[o1][this.activeKey2];
                value2 = this.sortKeys[o2][this.activeKey2];
            }
            return value2 - value1;

        }

        private int increment(int number) {
            return (number == this.sortKeys[0].length - 1) ? 0 : number + 1;
        }
    }

    private class AttributeRepresentant implements Comparable<AttributeRepresentant> {

        private int windowDistance;
        private IntArrayList numNewNonFds = new IntArrayList();
        private IntArrayList numComparisons = new IntArrayList();
        private float efficiencyFactor;
        private List<IntArrayList> clusters;
        private FDSet negCover;
        private FDTree posCover;
        private Sampler sampler;
        private MemoryGuardian memoryGuardian;

        public AttributeRepresentant(Collection<IntArrayList> clusters, float efficiencyFactor, FDSet negCover, FDTree posCover, Sampler sampler, MemoryGuardian memoryGuardian) {
            this.clusters = new ArrayList<>(clusters);
            this.efficiencyFactor = efficiencyFactor;
            this.negCover = negCover;
            this.posCover = posCover;
            this.sampler = sampler;
            this.memoryGuardian = memoryGuardian;
        }

        public float getEfficiencyFactor() {
            return this.efficiencyFactor;
        }

        public void setEfficiencyFactor(float efficiencyFactor) {
            this.efficiencyFactor = efficiencyFactor;
        }

        public int getEfficiency() { // TODO: If we keep calculating the efficiency with all comparisons and all results in the log, then we can also aggregate all comparisons and results in two variables without maintaining the entire log
            int sumNonFds = 0;
            int sumComparisons = 0;
            int index = this.numNewNonFds.size() - 1;
            while ((index >= 0) && (sumComparisons < this.efficiencyFactor)) {
                sumNonFds += this.numNewNonFds.getInt(index);
                sumComparisons += this.numComparisons.getInt(index);
                index--;
            }
            if (sumComparisons == 0)
                return 0;
            return (int) (sumNonFds * (this.efficiencyFactor / sumComparisons));
        }

        @Override
        public int compareTo(AttributeRepresentant o) {
//			return o.getNumNewNonFds() - this.getNumNewNonFds();		
            return (int) Math.signum(o.getEfficiency() - this.getEfficiency());
        }

        public boolean runNext(FDList newNonFds, int[][] compressedRecords) {
            this.windowDistance++;
            int numNewNonFds = 0;
            int numComparisons = 0;
            OpenBitSet equalAttrs = new OpenBitSet(this.posCover.getNumAttributes());

            int previousNegCoverSize = newNonFds.size();
            Iterator<IntArrayList> clusterIterator = this.clusters.iterator();
            while (clusterIterator.hasNext()) {
                IntArrayList cluster = clusterIterator.next();

                if (cluster.size() <= this.windowDistance) {
                    clusterIterator.remove();
                    continue;
                }

                for (int recordIndex = 0; recordIndex < (cluster.size() - this.windowDistance); recordIndex++) {
                    int recordId = cluster.getInt(recordIndex);
                    int partnerRecordId = cluster.getInt(recordIndex + this.windowDistance);

                    this.sampler.matcher.match(equalAttrs, recordId, partnerRecordId);

                    if (!this.negCover.contains(equalAttrs)) {
                        OpenBitSet equalAttrsCopy = equalAttrs.clone();
                        this.negCover.add(equalAttrsCopy);
                        newNonFds.add(equalAttrsCopy);

                        this.memoryGuardian.memoryChanged(1);
                        this.memoryGuardian.match(this.negCover, this.posCover, newNonFds);
                    }
                    numComparisons++;
                }
            }
            numNewNonFds = newNonFds.size() - previousNegCoverSize;

            this.numNewNonFds.add(numNewNonFds);
            this.numComparisons.add(numComparisons);

            return numComparisons != 0;
        }
    }


}
