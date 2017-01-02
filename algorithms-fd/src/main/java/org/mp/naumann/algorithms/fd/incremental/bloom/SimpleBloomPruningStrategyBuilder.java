package org.mp.naumann.algorithms.fd.incremental.bloom;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.utils.PowerSet;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SimpleBloomPruningStrategyBuilder extends BloomPruningStrategyBuilder {

    private static final int MAX_LEVEL = 4;
    private final int maxLevel;
    private final int numAttributes;

    public SimpleBloomPruningStrategyBuilder(List<String> columns, int maxLevel) {
        super(columns);
        this.maxLevel = maxLevel;
        this.numAttributes = columns.size();
    }

    public SimpleBloomPruningStrategyBuilder(List<String> columns) {
        this(columns, MAX_LEVEL);
    }

    @Override
    protected Set<OpenBitSet> generateCombinations() {
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
