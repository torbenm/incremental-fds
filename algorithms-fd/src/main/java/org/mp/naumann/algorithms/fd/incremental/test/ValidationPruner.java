package org.mp.naumann.algorithms.fd.incremental.test;

public interface ValidationPruner {

    boolean cannotBeViolated(LatticeElementLhsPair fd);

}
