package org.mp.naumann.algorithms.fd.incremental.pruning.bloom;

import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.function.IntConsumer;
import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.utils.PowerSet;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AllCombinationsBloomGenerator implements BloomGenerator {

    private static final int MAX_LEVEL = 2;
    private final int maxLevel;
    private int numAttributes;

    public AllCombinationsBloomGenerator(int maxLevel) {
        this.maxLevel = maxLevel;
    }

    public AllCombinationsBloomGenerator() {
        this(MAX_LEVEL);
    }

    @Override
    public Set<OpenBitSet> generateCombinations(List<String> columns) {
        this.numAttributes = columns.size();
        IntSet columnSet = IntStream.range(0, numAttributes).boxed().collect(Collectors.toCollection(
            IntOpenHashSet::new));
        int maxLevel = Math.min(numAttributes, this.maxLevel);
        return PowerSet.getPowerSet(columnSet, maxLevel).stream().map(this::toBitSet).collect(Collectors.toSet());
    }

    private OpenBitSet toBitSet(IntCollection cols) {
        OpenBitSet bits = new OpenBitSet(numAttributes);
        cols.forEach((IntConsumer) bits::fastSet);
        return bits;
    }
}
