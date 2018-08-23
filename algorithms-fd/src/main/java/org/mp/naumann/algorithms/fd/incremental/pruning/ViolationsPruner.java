package org.mp.naumann.algorithms.fd.incremental.pruning;

import java.util.Collection;
import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

public class ViolationsPruner implements ValidationPruner {

	private final Collection<OpenBitSetFD> tracked;

	public ViolationsPruner(Collection<OpenBitSetFD> tracked) {
		this.tracked = tracked;
	}

	@Override
	public boolean doesNotNeedValidation(OpenBitSet lhs, OpenBitSet rhs) {
		for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
			OpenBitSetFD fd = new OpenBitSetFD(lhs, rhsAttr);
			if (doesNeedValidation(fd)) {
				return false;
			}
		}
//		System.out.println("Pruned");
		return true;
	}

	private boolean doesNeedValidation(OpenBitSetFD fd) {
		return !tracked.contains(fd);
	}
}
