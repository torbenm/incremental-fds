package org.mp.naumann.algorithms.fd.incremental.bloom;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;
import org.mp.naumann.algorithms.fd.utils.FDTreeUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CurrentFDBloomGenerator implements BloomGenerator {

    private static final int MAX_LEVEL = Integer.MAX_VALUE;
    private final FDTree posCover;
    private final int maxLevel;

    public CurrentFDBloomGenerator(FDTree posCover, int maxLevel) {
        this.posCover = posCover;
        this.maxLevel = maxLevel;
    }

    public CurrentFDBloomGenerator(FDTree posCover) {
        this(posCover, MAX_LEVEL);
    }

    @Override
    public Set<OpenBitSet> generateCombinations(List<String> columns) {
        Set<OpenBitSet> bloomFds = new HashSet<>();
        int maxLevel = Math.min(posCover.getDepth(), this.maxLevel);
        for (int level = 0; level <= maxLevel; level++) {
            List<FDTreeElementLhsPair> currentLevel = FDTreeUtils.getFdLevel(posCover, level);
            for (FDTreeElementLhsPair fd : currentLevel) {
                bloomFds.add(fd.getLhs());
            }
        }
        return bloomFds;
    }
}
