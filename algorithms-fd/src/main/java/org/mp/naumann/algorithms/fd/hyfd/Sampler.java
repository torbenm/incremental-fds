package org.mp.naumann.algorithms.fd.hyfd;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.ClusterComparator;
import org.mp.naumann.algorithms.fd.structures.FDList;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.plis.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.plis.PliCollection;
import org.mp.naumann.algorithms.fd.utils.MemoryGuardian;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.logging.Logger;

class Sampler {

	private final static Logger LOG = Logger.getLogger(HyFD.class.getName());
	
	private final FDSet negCover;
	private final FDTree posCover;
	private final PliCollection plis;
	private final float efficiencyThreshold;
	private final ValueComparator valueComparator;
    private final MemoryGuardian memoryGuardian;
	private List<AttributeRepresentant> attributeRepresentants = null;


	Sampler(FDSet negCover, FDTree posCover, PliCollection plis, float efficiencyThreshold, ValueComparator valueComparator, MemoryGuardian memoryGuardian) {
		this.negCover = negCover;
		this.posCover = posCover;
		this.plis = plis;
		this.efficiencyThreshold = efficiencyThreshold;
		this.valueComparator = valueComparator;
		this.memoryGuardian = memoryGuardian;
	}

	FDList enrichNegativeCover(List<IntegerPair> comparisonSuggestions) {
		int numAttributes = plis.getNumberOfAttributes();
		
		LOG.info("Investigating comparison suggestions ... ");
		FDList newNonFds = new FDList(numAttributes, this.negCover.getMaxDepth());
		OpenBitSet equalAttrs = new OpenBitSet(this.posCover.getNumAttributes());

		for (IntegerPair comparisonSuggestion : comparisonSuggestions) {
			this.match(equalAttrs, comparisonSuggestion.a(), comparisonSuggestion.b());
			addToNegativeCoverIfDoesNotContains(newNonFds, equalAttrs);
		}
		
		if (this.attributeRepresentants == null) { // if this is the first call of this method
            initializeAttributeRepresentants(numAttributes, newNonFds);
		}
		else {
			// Lower the efficiency factor for this round
			for (AttributeRepresentant attributeRepresentant : this.attributeRepresentants) {
				attributeRepresentant.setEfficiencyFactor(attributeRepresentant.getEfficiencyFactor() * 2);
                // TODO: find a more clever way to increase the efficiency expectation
			}
		}
		
		LOG.info("Moving window over clusters ... ");
		PriorityQueue<AttributeRepresentant> queue = new PriorityQueue<>(this.attributeRepresentants);
		while (!queue.isEmpty()) {
			AttributeRepresentant attributeRepresentant = queue.remove();
			if (!attributeRepresentant.runNext(newNonFds, plis.getCompressed()))
				continue;
			
			if (attributeRepresentant.getEfficiency() != 0)
				queue.add(attributeRepresentant);
		}
		
		return newNonFds;
	}

    private void initializeAttributeRepresentants(int numAttributes, FDList newNonFds){
        LOG.info("Sorting clusters ...");
        long time = System.currentTimeMillis();
        ClusterComparator comparator = new ClusterComparator(plis.getCompressed(), plis.getNumberOfAttributes() - 1, 1);
        for (PositionListIndex pli : this.plis) {
            for (IntArrayList cluster : pli.getClusters()) {
                Collections.sort(cluster, comparator);
            }
            comparator.incrementActiveKey();
        }
        LOG.info("(" + (System.currentTimeMillis() - time) + "ms)");

        LOG.info("Running initial windows ...");
        time = System.currentTimeMillis();
        this.attributeRepresentants = new ArrayList<>(numAttributes);
        float efficiencyFactor = (int)Math.ceil(1 / this.efficiencyThreshold);
        for (int i = 0; i < numAttributes; i++) {
            AttributeRepresentant attributeRepresentant = new AttributeRepresentant(this.plis.get(i).getClusters(), efficiencyFactor, this.posCover, this);
            attributeRepresentant.runNext(newNonFds, plis.getCompressed());
            if (attributeRepresentant.getEfficiency() != 0)
                this.attributeRepresentants.add(attributeRepresentant);
        }
        LOG.info("(" + (System.currentTimeMillis() - time) + "ms)");
    }

	private void match(OpenBitSet equalAttrs, int t1, int t2) {
		this.match(equalAttrs, plis.getCompressed()[t1], plis.getCompressed()[t2]);
	}

    void match(OpenBitSet equalAttrs, int[] t1, int[] t2) {
		equalAttrs.clear(0, t1.length);
		for (int i = 0; i < t1.length; i++)
			if (this.valueComparator.isEqual(t1[i], t2[i]))
				equalAttrs.set(i);
	}

    void addToNegativeCoverIfDoesNotContains(FDList newNonFds, OpenBitSet equalAttrs){
        if (!this.negCover.contains(equalAttrs)) {
            OpenBitSet equalAttrsCopy = equalAttrs.clone();
            this.negCover.add(equalAttrsCopy);
            newNonFds.add(equalAttrsCopy);

            this.memoryGuardian.memoryChanged(1);
            this.memoryGuardian.match(this.negCover, this.posCover, newNonFds);
        }
    }
}
