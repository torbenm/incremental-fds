package org.mp.naumann.algorithms.fd.incremental.pruning;

import org.apache.lucene.util.OpenBitSet;
import org.junit.Test;
import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;
import org.mp.naumann.algorithms.fd.incremental.pruning.annotation.DefaultViolationSet;
import org.mp.naumann.algorithms.fd.incremental.pruning.annotation.DeletePruner;
import org.mp.naumann.algorithms.fd.incremental.pruning.annotation.SimpleDeleteValidationPruner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DeletePrunerTest {

    @Test
    public void test() {
        int numAttributes = 4;
        DeletePruner deletePruner = new DeletePruner(numAttributes, DefaultViolationSet::new, SimpleDeleteValidationPruner::new);
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
        ValidationPruner pruner = deletePruner.analyzeDiff(new CompressedDiff(new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>()));
        assertTrue(pruner.doesNotNeedValidation(toCheck, rhs));
        Map<Integer, int[]> deletes = new HashMap<>();
        deletes.put(1, null);
        pruner = deletePruner.analyzeDiff(new CompressedDiff(new HashMap<>(), deletes, new HashMap<>(), new HashMap<>()));
        assertTrue(pruner.doesNotNeedValidation(toCheck, rhs));
        deletes.put(2, null);
        pruner = deletePruner.analyzeDiff(new CompressedDiff(new HashMap<>(), deletes, new HashMap<>(), new HashMap<>()));
        assertFalse(pruner.doesNotNeedValidation(toCheck, rhs));
    }
}
