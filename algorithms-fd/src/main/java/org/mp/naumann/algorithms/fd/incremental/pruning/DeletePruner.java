package org.mp.naumann.algorithms.fd.incremental.pruning;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class DeletePruner {

    public static final class Pair {
        private final int first;
        private final int second;

        private Pair(int a, int b) {
            int comp = Integer.compare(a, b);
            if (comp < 0) {
                first = a;
                second = b;
            } else {
                first = b;
                second = a;
            }
        }

        @Override
        public String toString() {
            return "(" + first + "," + second + ")";
        }

        @Override
        public boolean equals(Object o) {

            if (o == this) {
                return true;
            }
            if (!(o instanceof Pair)) {
                return false;
            }

            Pair other = (Pair) o;

            return other.first == first && other.second == second;
        }

        @Override
        public int hashCode() {
            int result = 17;
            result = 31 * result + Integer.hashCode(first);
            result = 31 * result + Integer.hashCode(second);
            return result;
        }

        public boolean contains(int a) {
            return first == a || second == a;
        }
    }

    public interface CollectionCreator {
        Collection<Pair> createCollection();
    }

    private final Map<OpenBitSet, Collection<Pair>> violations = new HashMap<>();
    private final Multimap<Integer, OpenBitSet> index = HashMultimap.create();
    private final int numAttributes;
    private final CollectionCreator collectionCreator;

    public DeletePruner(int numAttributes, CollectionCreator collectionCreator) {
        this.numAttributes = numAttributes;
        this.collectionCreator = collectionCreator;
    }

    public void addAgreeSet(OpenBitSet agreeSet, int rec1, int rec2) {
        Collection<Pair> set = violations.computeIfAbsent(agreeSet, k -> collectionCreator.createCollection());
        set.add(new Pair(rec1, rec2));
        index.put(rec1, agreeSet);
        index.put(rec2, agreeSet);
    }

    public ValidationPruner analyzeDiff(CompressedDiff diff) {
        for (int delete : diff.getDeletedRecords().keySet()) {
            for (OpenBitSet agreeSet : index.get(delete)) {
                Collection<Pair> set = violations.get(agreeSet);
                if (set != null) {
                    set.removeIf(pair -> pair.contains(delete));
                    if (set.isEmpty()) {
                        violations.remove(agreeSet);
                    }
                }
            }
            index.removeAll(delete);
        }
        CardinalitySet remainingViolations = new CardinalitySet(numAttributes);
        for (Entry<OpenBitSet, Collection<Pair>> violation : violations.entrySet()) {
            if (!violation.getValue().isEmpty()) {
                remainingViolations.add(violation.getKey());
            }
        }
        return new DeleteValidationPruner(remainingViolations);
    }

    private static class DeleteValidationPruner implements ValidationPruner {

        private final CardinalitySet violations;

        private DeleteValidationPruner(CardinalitySet violations) {
            this.violations = violations;
        }

        @Override
        public boolean doesNotNeedValidation(OpenBitSet lhs, OpenBitSet originalRhs) {
            OpenBitSet rhs = originalRhs.clone();
            for (int level = violations.getDepth(); level >= lhs.cardinality(); level--) {
                ObjectOpenHashSet<OpenBitSet> violationLevel = violations.getLevel(level);
                for (OpenBitSet violation : violationLevel) {
                    if (BitSetUtils.isContained(lhs, violation)) {
                        // records agree in lhs
                        // remove bits from rhs where records disagree
                        rhs.and(violation);
                        if (rhs.isEmpty()) {
                            // there was disagreement for every rhs bit -> fd still invalid
                            return true;
                        }
                    }
                }
            }
            return false;
        }

    }
}
