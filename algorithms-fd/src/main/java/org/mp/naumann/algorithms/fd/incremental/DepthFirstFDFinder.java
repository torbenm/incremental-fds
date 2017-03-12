package org.mp.naumann.algorithms.fd.incremental;

import java.util.List;
import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.ActualValidator.ValidationCallback;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.Lattice;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

public class DepthFirstFDFinder {

    private final IncrementalInductor inductor;
    private final List<? extends PositionListIndex> plis;
    private final CompressedRecords compressedRecords;
    private final int numRecords;
    private final Lattice posCover;
    private final int numAttributes;

    DepthFirstFDFinder(Lattice posCover, Lattice negCover,
        List<? extends PositionListIndex> plis, CompressedRecords compressedRecords,
        int numRecords) {
        this.numAttributes = compressedRecords.getNumAttributes();
        this.plis = plis;
        this.compressedRecords = compressedRecords;
        this.numRecords = numRecords;
        this.posCover = posCover;
        this.inductor = new IncrementalInductor(negCover, posCover, numAttributes);
    }

    public void findFDs(List<OpenBitSetFD> fds) {
        for (OpenBitSetFD fd : fds) {
            int rhs = fd.getRhs();
            OpenBitSet lhs = fd.getLhs().clone();
            lhs.flip(0, numAttributes);
            generalize(lhs, rhs, 0);
        }
    }

    private void generalize(OpenBitSet lhs, int rhs, int nextLhsAttr) {
        int lhsAttr = lhs.nextSetBit(nextLhsAttr);
        if (lhsAttr < 0) {
            deduceDependencies(lhs, rhs);
            return;
        }
        OpenBitSet genLhs = lhs.clone();
        genLhs.fastClear(lhsAttr);
        if (!posCover.containsFdOrGeneralization(genLhs, rhs)) {
            ValidationCallback valid = (_lhs, rhsAttr) -> generalize(genLhs, rhs, lhsAttr + 1);
            ValidationCallback invalid = (_lhs, rhsAttr) -> deduceDependencies(lhs, rhs);
            ActualValidator validator = new ActualValidator(plis, compressedRecords, numRecords,
                valid, invalid, true);
            validator.validate(genLhs, rhs);
        }
        generalize(lhs, rhs, lhsAttr + 1);
    }

    private void deduceDependencies(OpenBitSet lhs, int rhs) {
        OpenBitSet flipped = lhs.clone();
        flipped.flip(0, numAttributes);
        inductor.deduceDependencies(flipped, rhs);
    }

}
