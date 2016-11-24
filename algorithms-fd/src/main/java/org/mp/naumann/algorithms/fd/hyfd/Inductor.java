package org.mp.naumann.algorithms.fd.hyfd;

import java.util.List;
import java.util.logging.Logger;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.FDList;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.utils.MemoryGuardian;

class Inductor {

	private final static Logger LOG = Logger.getLogger(HyFD.class.getName());
	
	private FDSet negCover;
	private FDTree posCover;
	private MemoryGuardian memoryGuardian;

	Inductor(FDSet negCover, FDTree posCover, MemoryGuardian memoryGuardian) {
		this.negCover = negCover;
		this.posCover = posCover;
		this.memoryGuardian = memoryGuardian;
	}

	void updatePositiveCover(FDList nonFds) {
		LOG.info("Inducing OpenBitFunctionalDependency candidates ...");
		for (int i = nonFds.getFdLevels().size() - 1; i >= 0; i--) {
			if (i >= nonFds.getFdLevels().size()) // If this level has been trimmed during iteration
				continue;
			
			List<OpenBitSet> nonFdLevel = nonFds.getFdLevels().get(i);
			for (OpenBitSet lhs : nonFdLevel) {
				
				OpenBitSet fullRhs = lhs.clone();
				fullRhs.flip(0, fullRhs.size());
				
				for (int rhs = fullRhs.nextSetBit(0); rhs >= 0; rhs = fullRhs.nextSetBit(rhs + 1))
					this.specializePositiveCover(lhs, rhs, nonFds);
			}
			nonFdLevel.clear();
		}
	}
	
	private int specializePositiveCover(OpenBitSet lhs, int rhs, FDList nonFds) {
		int numAttributes = this.posCover.getChildren().length;
		int newFDs = 0;
		List<OpenBitSet> specLhss;
		
		if (!(specLhss = this.posCover.getFdAndGeneralizations(lhs, rhs)).isEmpty()) { // TODO: May be "while" instead of "if"?
			for (OpenBitSet specLhs : specLhss) {
				this.posCover.removeFunctionalDependency(specLhs, rhs);
				
				if ((this.posCover.getMaxDepth() > 0) && (specLhs.cardinality() >= this.posCover.getMaxDepth()))
					continue;
				
				for (int attr = numAttributes - 1; attr >= 0; attr--) { // TODO: Is iterating backwards a good or bad idea?
					if (!lhs.get(attr) && (attr != rhs)) {
						specLhs.set(attr);
						if (!this.posCover.containsFdOrGeneralization(specLhs, rhs)) {
							this.posCover.addFunctionalDependency(specLhs, rhs);
							newFDs++;
							
							// If dynamic memory management is enabled, frequently check the memory consumption and trim the positive cover if it does not fit anymore
							notifyMemoryGuardianOfChange(nonFds);
						}
						specLhs.clear(attr);
					}
				}
			}
		}
		return newFDs;
	}

	private void notifyMemoryGuardianOfChange(FDList nonFds){
		this.memoryGuardian.memoryChanged(1);
		this.memoryGuardian.match(this.negCover, this.posCover, nonFds);
	}
}


