package org.mp.naumann.algorithms.fd.incremental;

import org.apache.lucene.util.OpenBitSet;

public class BitSetUtils {

	public static boolean isContained(OpenBitSet a, OpenBitSet b) {
		return OpenBitSet.andNotCount(a, b) == 0;
	}

}
