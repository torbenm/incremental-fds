package org.mp.naumann.algorithms.fd.incremental;

import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;

public interface PruningStrategy {

    boolean cannotBeViolated(FDTreeElementLhsPair fd);

}
