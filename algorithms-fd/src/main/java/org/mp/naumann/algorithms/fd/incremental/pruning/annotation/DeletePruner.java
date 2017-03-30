package org.mp.naumann.algorithms.fd.incremental.pruning.annotation;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;
import org.mp.naumann.algorithms.fd.incremental.pruning.CardinalitySet;
import org.mp.naumann.algorithms.fd.incremental.pruning.ValidationPruner;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class DeletePruner {

    private final Map<OpenBitSet, ViolationSet> violations = new HashMap<>();
    private final Multimap<Integer, OpenBitSet> index = HashMultimap.create();
    private final int numAttributes;
    private final CollectionCreator collectionCreator;
    private final ValidationPrunerCreator validationPrunerCreator;
    public DeletePruner(int numAttributes, CollectionCreator collectionCreator, ValidationPrunerCreator validationPrunerCreator) {
        this.numAttributes = numAttributes;
        this.collectionCreator = collectionCreator;
        this.validationPrunerCreator = validationPrunerCreator;
    }

    public void addAgreeSet(OpenBitSet agreeSet, int rec1, int rec2) {
        ViolationSet set = violations.computeIfAbsent(agreeSet, k -> collectionCreator.createCollection());
        set.add(new ViolatingPair(rec1, rec2));
        index.put(rec1, agreeSet);
        index.put(rec2, agreeSet);
    }

    public ValidationPruner analyzeDiff(CompressedDiff diff) {
        Set<OpenBitSet> agreeSets = new HashSet<>(violations.size());
        Set<Integer> deleted = diff.getDeletedRecords().keySet();
        for (int delete : deleted) {
            Collection<OpenBitSet> removed = index.removeAll(delete);
            agreeSets.addAll(removed);
        }
        for (OpenBitSet agreeSet : agreeSets) {
            ViolationSet set = violations.get(agreeSet);
            if (set != null) {
                Iterator<ViolatingPair> it = set.iterator();
                while (it.hasNext()) {
                    ViolatingPair pair = it.next();
                    if (pair.intersects(deleted)) {
                        it.remove();
                    }
                }
                if (set.isEmpty()) {
                    violations.remove(agreeSet);
                }
            }
        }
        CardinalitySet remainingViolations = new CardinalitySet(numAttributes);
        for (Entry<OpenBitSet, ViolationSet> violation : violations.entrySet()) {
            if (!violation.getValue().isEmpty()) {
                remainingViolations.add(violation.getKey());
            }
        }
        return validationPrunerCreator.createValidationPruner(remainingViolations);
    }

    public interface ViolationSet extends Iterable<ViolatingPair> {

        void add(ViolatingPair violatingPair);

        boolean isEmpty();
    }

    public interface CollectionCreator {
        ViolationSet createCollection();
    }

    public interface ValidationPrunerCreator {
        ValidationPruner createValidationPruner(CardinalitySet remaningValues);
    }

}
