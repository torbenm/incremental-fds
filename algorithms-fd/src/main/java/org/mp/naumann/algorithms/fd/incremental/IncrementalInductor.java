package org.mp.naumann.algorithms.fd.incremental;

import java.util.List;
import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.Lattice;

class IncrementalInductor {

    private final Lattice inverseLattice;
    private final Lattice lattice;
    private final int numAttributes;

    IncrementalInductor(Lattice lattice, Lattice inverseLattice, int numAttributes) {
        this.inverseLattice = inverseLattice;
        this.lattice = lattice;
        this.numAttributes = numAttributes;
    }

    void deduceDependencies(OpenBitSet lhs, int rhs) {
        List<OpenBitSet> specLhss = this.lattice.getFdAndGeneralizations(lhs, rhs);
        if (!specLhss.isEmpty()) { // TODO: May be "while" instead of "if"?
            for (OpenBitSet specLhs : specLhss) {
                this.lattice.removeFunctionalDependency(specLhs, rhs);

                OpenBitSet flipped = flip(specLhs);
                if (!inverseLattice.containsFdOrGeneralization(flipped, rhs)) {
                    inverseLattice.addFunctionalDependency(flipped, rhs);
                    inverseLattice.removeSpecializations(flipped, rhs);
                }

                for (int attr = numAttributes - 1; attr >= 0; attr--) { // TODO: Is iterating backwards a good or bad idea?
                    if (!lhs.get(attr) && (attr != rhs)) {
                        specLhs.fastSet(attr);

                        if (!this.lattice.containsFdOrGeneralization(specLhs, rhs)) {
                            this.lattice.addFunctionalDependency(specLhs, rhs);
                        }
                        specLhs.fastClear(attr);
                    }
                }
            }
        }
    }

    private OpenBitSet flip(OpenBitSet lhs) {
        OpenBitSet flipped = lhs.clone();
        flipped.flip(0, numAttributes);
        return flipped;
    }

}
