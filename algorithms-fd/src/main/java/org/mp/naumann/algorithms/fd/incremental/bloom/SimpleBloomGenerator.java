package org.mp.naumann.algorithms.fd.incremental.bloom;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.utils.PowerSet;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SimpleBloomGenerator implements BloomGenerator {

    private static final int MAX_LEVEL = 2;
    private final int maxLevel;
    private int numAttributes;

    public SimpleBloomGenerator(int maxLevel) {
        this.maxLevel = maxLevel;
    }

    public SimpleBloomGenerator() {
        this(MAX_LEVEL);
    }

    @Override
    public Set<OpenBitSet> generateCombinations(List<String> columns) {
        this.numAttributes = columns.size();
        Set<Integer> columnSet = IntStream.range(0, numAttributes).boxed().collect(Collectors.toSet());
        int maxLevel = Math.min(numAttributes, this.maxLevel);
        return PowerSet.getPowerSet(columnSet, maxLevel).stream().map(this::toBitSet).collect(Collectors.toSet());
    }

    private OpenBitSet toBitSet(Collection<Integer> cols) {
        OpenBitSet bits = new OpenBitSet(numAttributes);
        for (Integer column : cols) {
            bits.fastSet(column);
        }
        return bits;
    }
}
