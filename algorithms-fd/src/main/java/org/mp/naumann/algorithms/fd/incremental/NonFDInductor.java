package org.mp.naumann.algorithms.fd.incremental;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.benchmark.better.Benchmark;
import org.mp.naumann.algorithms.fd.incremental.ActualValidator.ValidationCallback;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.Lattice;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

public class NonFDInductor {

    private final IncrementalInductor inductor;
    private final List<? extends PositionListIndex> plis;
    private final CompressedRecords compressedRecords;
    private final int numRecords;
    private final Lattice posCover;
    private final int numAttributes;

    NonFDInductor(Lattice posCover, Lattice negCover,
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
        Benchmark benchmark = Benchmark.start("Depth first for " + fds.size() + " FDs");
        int i = 0;
        int successes = 0;
        int total = 0;
        for (OpenBitSetFD fd : fds) {
            int rhs = fd.getRhs();
            OpenBitSet lhs = fd.getLhs().clone();
            lhs.flip(0, numAttributes);
            int newFds = generalize(lhs, rhs, 0);
            total += newFds;
            if (newFds != 0) {
                successes++;
            }
            i++;
        }
        System.out.println(successes + " successes out of " + i);
        System.out.println("Found " + total + " new FDs");
        benchmark.finish();
    }

    private int generalize(OpenBitSet lhs, int rhs, int nextLhsAttr) {
        for (int lhsAttr = lhs.nextSetBit(nextLhsAttr); lhsAttr >= 0;
            lhsAttr = lhs.nextSetBit(lhsAttr + 1)) {
            AtomicBoolean wasValid = new AtomicBoolean();
            OpenBitSet genLhs = lhs.clone();
            genLhs.fastClear(lhsAttr);
            if (!posCover.containsFdOrGeneralization(genLhs, rhs)) {
                ValidationCallback valid = (_lhs, rhsAttr, collectedFDs) -> wasValid.set(true);
                ValidationCallback invalid = (_lhs, rhsAttr, collectedFDs) -> wasValid.set(false);
                ActualValidator validator = new ActualValidator(plis, compressedRecords, numRecords,
                    valid, invalid, true);
                validator.validate(genLhs, rhs);
                if (wasValid.get()) {
                    return generalize(genLhs, rhs, lhsAttr + 1);
                }
            }
        }
        return deduceDependencies(lhs, rhs);
    }

    private int deduceDependencies(OpenBitSet lhs, int rhs) {
        OpenBitSet flipped = lhs.clone();
        flipped.flip(0, numAttributes);
        return inductor.deduceDependencies(flipped, rhs);
    }

}
