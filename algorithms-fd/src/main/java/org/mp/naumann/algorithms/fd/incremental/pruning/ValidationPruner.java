package org.mp.naumann.algorithms.fd.incremental.pruning;

import org.apache.lucene.util.OpenBitSet;

public interface ValidationPruner {

    boolean doesNotNeedValidation(OpenBitSet lhs, OpenBitSet rhs);

}
