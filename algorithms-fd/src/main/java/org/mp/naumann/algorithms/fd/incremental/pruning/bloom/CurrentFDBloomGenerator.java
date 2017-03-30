package org.mp.naumann.algorithms.fd.incremental.pruning.bloom;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.structures.Lattice;
import org.mp.naumann.algorithms.fd.incremental.structures.LatticeElementLhsPair;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CurrentFDBloomGenerator implements BloomGenerator {

    private static final int MAX_LEVEL = Integer.MAX_VALUE;
    private final Lattice fds;
    private final int maxLevel;

    private CurrentFDBloomGenerator(Lattice fds, int maxLevel) {
        this.fds = fds;
        this.maxLevel = maxLevel;
    }

    public CurrentFDBloomGenerator(Lattice fds) {
        this(fds, MAX_LEVEL);
    }

    @Override
    public Set<OpenBitSet> generateCombinations(List<String> columns) {
        Set<OpenBitSet> bloomFds = new HashSet<>();
        int maxLevel = Math.min(fds.getDepth(), this.maxLevel);
        for (int level = 0; level <= maxLevel; level++) {
            Collection<LatticeElementLhsPair> currentLevel = fds.getLevel(level);
            for (LatticeElementLhsPair fd : currentLevel) {
                bloomFds.add(fd.getLhs());
            }
        }
        return bloomFds;
    }
}
