package org.mp.naumann.algorithms.fd.incremental;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.benchmark.speed.Benchmark;
import org.mp.naumann.algorithms.fd.incremental.structures.Lattice;

import java.util.List;

class IncrementalInductor {

    private final Lattice inverseLattice;
    private final Lattice lattice;
    private final int numAttributes;

    IncrementalInductor(Lattice lattice, Lattice inverseLattice, int numAttributes) {
        this.inverseLattice = inverseLattice;
        this.lattice = lattice;
        this.numAttributes = numAttributes;
    }

    int deduceDependencies(OpenBitSet lhs, int rhs) {
        Benchmark benchmark = Benchmark.start("Deduction", Benchmark.DEFAULT_LEVEL + 5);
        int newFDs = 0;
        List<OpenBitSet> specLhss = this.lattice.getFdAndGeneralizations(lhs, rhs);
        benchmark.finishSubtask("Found " + specLhss.size() + " covered");
        if (!specLhss.isEmpty()) { // TODO: May be "while" instead of "if"?
            OpenBitSet flipped = flip(lhs);
            if (!inverseLattice.containsFdOrGeneralization(flipped, rhs)) {
                inverseLattice.addFunctionalDependency(flipped, rhs);
                inverseLattice.removeSpecializations(flipped, rhs);
            }
            for (OpenBitSet specLhs : specLhss) {
                this.lattice.removeFunctionalDependency(specLhs, rhs);

                for (int attr = numAttributes - 1; attr >= 0; attr--) { // TODO: Is iterating backwards a good or bad idea?
                    if (!lhs.get(attr) && (attr != rhs)) {
                        specLhs.fastSet(attr);

                        if (!this.lattice.containsFdOrGeneralization(specLhs, rhs)) {
                            this.lattice.addFunctionalDependency(specLhs, rhs);
                            newFDs++;
                        }
                        specLhs.fastClear(attr);
                    }
                }
            }
            benchmark.finishSubtask("Deduced " + newFDs + " new FDs");
        }
        benchmark.finish();
        return newFDs;
    }

    private OpenBitSet flip(OpenBitSet lhs) {
        OpenBitSet flipped = lhs.clone();
        flipped.flip(0, numAttributes);
        return flipped;
    }

}
