package org.mp.naumann.algorithms.fd.incremental;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.benchmark.speed.Benchmark;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.hyfd.FDList;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

class IncrementalSampler {

    private static final boolean SORT_PARALLEL = true;
    private final FDSet agreeSets;
    private final CompressedRecords compressedRecords;
    private final List<? extends PositionListIndex> plis;
    private final float efficiencyThreshold;
    private final IncrementalMatcher matcher;
    private List<AttributeRepresentant> attributeRepresentants = null;
    private Collection<Integer> newRecords;

    IncrementalSampler(CompressedRecords compressedRecords, List<? extends PositionListIndex> plis, float efficiencyThreshold, IncrementalMatcher matcher) {
        int numAttributes = compressedRecords.getNumAttributes();
        this.agreeSets = new FDSet(numAttributes, -1);
        this.compressedRecords = compressedRecords;
        this.plis = plis;
        this.efficiencyThreshold = efficiencyThreshold;
        this.matcher = matcher;
    }

    void setNewRecords(Collection<Integer> newRecords) {
        this.newRecords = newRecords;
    }

    private boolean isOldRecord(int recordId) {
        return newRecords != null && !newRecords.contains(recordId);
    }

    FDList enrichNegativeCover(List<IntegerPair> comparisonSuggestions) {
        Benchmark benchmark = Benchmark.start("Sampling", Benchmark.DEFAULT_LEVEL + 3);
        int numAttributes = this.compressedRecords.getNumAttributes();

        FDLogger.log(Level.FINEST, "Investigating comparison suggestions ... ");
        FDList newNonFds = new FDList(numAttributes, this.agreeSets.getMaxDepth());
        OpenBitSet equalAttrs = new OpenBitSet(this.compressedRecords.getNumAttributes());
        for (IntegerPair comparisonSuggestion : comparisonSuggestions) {
            this.matcher.match(equalAttrs, comparisonSuggestion.a(), comparisonSuggestion.b());

            if (!this.agreeSets.contains(equalAttrs)) {
                OpenBitSet equalAttrsCopy = equalAttrs.clone();
                this.agreeSets.add(equalAttrsCopy);
                newNonFds.add(equalAttrsCopy);
            }
        }

        benchmark.finishSubtask("Processed comparison suggestions");
        if (this.attributeRepresentants == null) { // if this is the first call of this method
            FDLogger.log(Level.FINEST, "Running initial windows ...");
            this.attributeRepresentants = new ArrayList<>(numAttributes);
            float efficiencyFactor = (int) Math.ceil(1 / this.efficiencyThreshold);
            ClusterComparator comparator = new ClusterComparator(this.compressedRecords, this.compressedRecords.getNumAttributes() - 1, 1);
            for (PositionListIndex pli : this.plis) {
                Benchmark pliBenchmark = Benchmark.start("Sampling PLI " + pli.getAttribute(), Benchmark.DEFAULT_LEVEL + 4);
                Iterator<? extends Collection<Integer>> it = pli.getClustersToCheck(true);
                final List<IntArrayList> clusters;
                if (SORT_PARALLEL) {
                    clusters = StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, Spliterator.CONCURRENT), true).map(c -> sort(comparator, c)).collect(Collectors.toList());
                } else {
                    clusters = new ArrayList<>();
                    while (it.hasNext()) {
                        clusters.add(sort(comparator, it.next()));
                    }
                }
                pliBenchmark.finishSubtask("Sorting");
                comparator.incrementActiveKey();
                AttributeRepresentant attributeRepresentant = new AttributeRepresentant(clusters, efficiencyFactor, this.agreeSets, this);
                attributeRepresentant.runNext(newNonFds, this.compressedRecords);
                if (attributeRepresentant.getEfficiency() != 0) {
                    this.attributeRepresentants.add(attributeRepresentant);
                }
                pliBenchmark.finishSubtask("Run");
                pliBenchmark.finish();
            }
            benchmark.finishSubtask("Initial windows");
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
            if (!attributeRepresentant.runNext(newNonFds, this.compressedRecords)) {
                continue;
            }

            if (attributeRepresentant.getEfficiency() != 0) {
                queue.add(attributeRepresentant);
            }
        }
        benchmark.finishSubtask("Next windows");
        benchmark.finish();

        return newNonFds;
    }

    private static IntArrayList sort(Comparator<Integer> comparator, Collection<Integer> collection) {
        IntArrayList list = new IntArrayList(collection);
        list.sort(comparator);
        return list;
    }

    private class ClusterComparator implements Comparator<Integer> {

        private final CompressedRecords sortKeys;
        private int activeKey1;
        private int activeKey2;

        ClusterComparator(CompressedRecords sortKeys, int activeKey1, int activeKey2) {
            super();
            this.sortKeys = sortKeys;
            this.activeKey1 = activeKey1;
            this.activeKey2 = activeKey2;
        }

        void incrementActiveKey() {
            this.activeKey1 = this.increment(this.activeKey1);
            this.activeKey2 = this.increment(this.activeKey2);
        }

        @Override
        public int compare(Integer o1, Integer o2) {
            // Previous -> Next
            int value1 = this.sortKeys.get(o1)[this.activeKey1];
            int value2 = this.sortKeys.get(o2)[this.activeKey1];
            int result = value2 - value1;
            if (result == 0) {
                value1 = this.sortKeys.get(o1)[this.activeKey2];
                value2 = this.sortKeys.get(o2)[this.activeKey2];
            }
            return value2 - value1;
        }

        private int increment(int number) {
            return (number == this.sortKeys.getNumAttributes() - 1) ? 0 : number + 1;
        }
    }

    private class AttributeRepresentant implements Comparable<AttributeRepresentant> {

        private final IntArrayList numNewNonFds = new IntArrayList();
        private final IntArrayList numComparisons = new IntArrayList();
        private final List<IntArrayList> clusters;
        private final FDSet negCover;
        private final IncrementalSampler sampler;
        private int windowDistance;
        private float efficiencyFactor;

        AttributeRepresentant(List<IntArrayList> clusters, float efficiencyFactor, FDSet negCover, IncrementalSampler sampler) {
            this.clusters = clusters;
            this.efficiencyFactor = efficiencyFactor;
            this.negCover = negCover;
            this.sampler = sampler;
        }

        float getEfficiencyFactor() {
            return this.efficiencyFactor;
        }

        void setEfficiencyFactor(float efficiencyFactor) {
            this.efficiencyFactor = efficiencyFactor;
        }

        int getEfficiency() { // TODO: If we keep calculating the efficiency with all comparisons and all results in the log, then we can also aggregate all comparisons and results in two variables without maintaining the entire log
            int sumNonFds = 0;
            int sumComparisons = 0;
            int index = this.numNewNonFds.size() - 1;
            while ((index >= 0) && (sumComparisons < this.efficiencyFactor)) {
                sumNonFds += this.numNewNonFds.getInt(index);
                sumComparisons += this.numComparisons.getInt(index);
                index--;
            }
            if (sumComparisons == 0) {
                return 0;
            }
            return (int) (sumNonFds * (this.efficiencyFactor / sumComparisons));
        }

        @Override
        public int compareTo(AttributeRepresentant o) {
//			return o.getNumNewNonFds() - this.getNumNewNonFds();		
            return (int) Math.signum(o.getEfficiency() - this.getEfficiency());
        }

        boolean runNext(FDList newNonFds, CompressedRecords compressedRecords) {
            this.windowDistance++;
            int numComparisons = 0;
            OpenBitSet equalAttrs = new OpenBitSet(compressedRecords.getNumAttributes());

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

                    if (isOldRecord(recordId) && isOldRecord(partnerRecordId)) {
                        continue;
                    }
                    this.sampler.matcher.match(equalAttrs, recordId, partnerRecordId);

                    if (!this.negCover.contains(equalAttrs)) {
                        OpenBitSet equalAttrsCopy = equalAttrs.clone();
                        this.negCover.add(equalAttrsCopy);
                        newNonFds.add(equalAttrsCopy);
                    }
                    numComparisons++;
                }
            }
            int numNewNonFds = newNonFds.size() - previousNegCoverSize;
            this.numNewNonFds.add(numNewNonFds);
            this.numComparisons.add(numComparisons);
            return numComparisons != 0;
        }
    }
}
