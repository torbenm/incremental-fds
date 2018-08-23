package org.mp.naumann.algorithms.fd.incremental;

import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

public class ComparisonSugestion {

	private final IntegerPair pair;
	private final OpenBitSetFD fd;

	public ComparisonSugestion(IntegerPair pair,
		OpenBitSetFD fd) {
		this.pair = pair;
		this.fd = fd;
	}

	public OpenBitSetFD getFd() {
		return fd;
	}

	public IntegerPair getPair() {
		return pair;
	}
}
