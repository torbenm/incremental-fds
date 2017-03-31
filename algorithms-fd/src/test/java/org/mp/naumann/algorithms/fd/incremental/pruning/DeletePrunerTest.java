package org.mp.naumann.algorithms.fd.incremental.pruning;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.util.OpenBitSet;
import org.junit.Test;
import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;
import org.mp.naumann.algorithms.fd.incremental.agreesets.DefaultViolationSet;
import org.mp.naumann.algorithms.fd.incremental.agreesets.AgreeSetCollection;
import org.mp.naumann.algorithms.fd.incremental.pruning.annotation.ExactDeleteValidationPruner;

public class DeletePrunerTest {

    @Test
    public void test() {
        int numAttributes = 4;
        AgreeSetCollection deletePruner = new AgreeSetCollection(DefaultViolationSet::new);
        OpenBitSet agreeSet = new OpenBitSet(numAttributes);
        agreeSet.fastSet(0);
        agreeSet.fastSet(1);
        agreeSet.fastSet(2);
        deletePruner.addAgreeSet(agreeSet, 0, 1);
        deletePruner.addAgreeSet(agreeSet, 0, 2);
        OpenBitSet toCheck = new OpenBitSet(numAttributes);
        toCheck.fastSet(0);
        toCheck.fastSet(1);
        OpenBitSet rhs = new OpenBitSet(numAttributes);
        rhs.fastSet(3);
        Set<OpenBitSet> agreeSets = deletePruner.analyzeDiff(new CompressedDiff(new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>()));
        ValidationPruner pruner = new ExactDeleteValidationPruner(agreeSets, numAttributes);
        assertTrue(pruner.doesNotNeedValidation(toCheck, rhs));
        Map<Integer, int[]> deletes = new HashMap<>();
        deletes.put(1, null);
        agreeSets = deletePruner.analyzeDiff(new CompressedDiff(new HashMap<>(), deletes, new HashMap<>(), new HashMap<>()));
        pruner = new ExactDeleteValidationPruner(agreeSets, numAttributes);
        assertTrue(pruner.doesNotNeedValidation(toCheck, rhs));
        deletes.put(2, null);
        agreeSets = deletePruner.analyzeDiff(new CompressedDiff(new HashMap<>(), deletes, new HashMap<>(), new HashMap<>()));
        pruner = new ExactDeleteValidationPruner(agreeSets, numAttributes);
        assertFalse(pruner.doesNotNeedValidation(toCheck, rhs));
    }
}
