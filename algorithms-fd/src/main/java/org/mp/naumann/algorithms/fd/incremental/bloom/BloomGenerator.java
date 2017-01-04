package org.mp.naumann.algorithms.fd.incremental.bloom;


import org.apache.lucene.util.OpenBitSet;

import java.util.List;
import java.util.Set;

public interface BloomGenerator {

    Set<OpenBitSet> generateCombinations(List<String> columns);

}
