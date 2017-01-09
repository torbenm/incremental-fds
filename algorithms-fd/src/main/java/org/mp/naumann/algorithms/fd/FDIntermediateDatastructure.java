package org.mp.naumann.algorithms.fd;

import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.hyfd.PLIBuilder;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;

public class FDIntermediateDatastructure {

	private final FDTree posCover;
	private final FDSet negCover;
	private final PLIBuilder pliBuilder;
	private final ValueComparator valueComparator;

	public FDIntermediateDatastructure(FDSet negCover, FDTree posCover, PLIBuilder pliBuilder, ValueComparator valueComparator) {
		this.posCover = posCover;
		this.pliBuilder = pliBuilder;
		this.negCover = negCover;
		this.valueComparator = valueComparator;
	}

	public FDTree getPosCover() {
		return posCover;
	}

	public FDSet getNegCover() {
		return negCover;
	}

	public PLIBuilder getPliBuilder() {
		return pliBuilder;
	}

	public ValueComparator getValueComparator() {
		return valueComparator;
	}
}
