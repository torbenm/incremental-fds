package org.mp.naumann.algorithms.fd.hyfd;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.FDList;
import org.mp.naumann.algorithms.fd.structures.FDTree;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class AttributeRepresentant  implements Comparable<AttributeRepresentant> {

    private int windowDistance;
    private IntArrayList numNewNonFds = new IntArrayList();
    private IntArrayList numComparisons = new IntArrayList();
    private float efficiencyFactor;
    private List<IntArrayList> clusters;
    private FDTree posCover;
    private Sampler sampler;

    AttributeRepresentant(List<IntArrayList> clusters, float efficiencyFactor, FDTree posCover, Sampler sampler) {
        this.clusters = new ArrayList<>(clusters);
        this.efficiencyFactor = efficiencyFactor;
        this.posCover = posCover;
        this.sampler = sampler;
    }


    float getEfficiencyFactor() {
        return this.efficiencyFactor;
    }

    void setEfficiencyFactor(float efficiencyFactor) {
        this.efficiencyFactor = efficiencyFactor;
    }

    int getEfficiency() {
        /* TODO: If we keep calculating the efficiency with all comparisons and
         * all results in the log, then we can also aggregate all comparisons
         * and results in two variables without maintaining the entire log
         */
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
        return (int)(sumNonFds * (this.efficiencyFactor / sumComparisons));
    }


    @Override
    public int compareTo(AttributeRepresentant o) {
        return (int)Math.signum(o.getEfficiency() - this.getEfficiency());
    }

    boolean runNext(FDList newNonFds, int[][] compressedRecords) {
        int numNewNonFds, numComparisons = 0;
        OpenBitSet equalAttrs = new OpenBitSet(this.posCover.getNumAttributes());
        int previousNegCoverSize = newNonFds.size();
        Iterator<IntArrayList> clusterIterator = this.clusters.iterator();

        this.windowDistance++;
        while (clusterIterator.hasNext()) {
            IntArrayList cluster = clusterIterator.next();

            if (cluster.size() <= this.windowDistance) {
                clusterIterator.remove();
                continue;
            }

            for (int recordIndex = 0; recordIndex < (cluster.size() - this.windowDistance); recordIndex++) {
                int recordId = cluster.getInt(recordIndex);
                int partnerRecordId = cluster.getInt(recordIndex + this.windowDistance);

                this.sampler.match(equalAttrs, compressedRecords[recordId], compressedRecords[partnerRecordId]);

                this.sampler.addToNegativeCoverIfDoesNotContains(newNonFds, equalAttrs);
                numComparisons++;
            }
        }
        numNewNonFds = newNonFds.size() - previousNegCoverSize;

        this.numNewNonFds.add(numNewNonFds);
        this.numComparisons.add(numComparisons);

        return numComparisons != 0;
    }
}
