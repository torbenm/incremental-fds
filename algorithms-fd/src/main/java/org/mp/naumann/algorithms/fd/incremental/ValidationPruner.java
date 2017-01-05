package org.mp.naumann.algorithms.fd.incremental;

import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;

public interface ValidationPruner {

    boolean cannotBeViolated(FDTreeElementLhsPair fd);

}
