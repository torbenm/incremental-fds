package org.mp.naumann.algorithms.fd.incremental.pruning;

import org.apache.lucene.util.OpenBitSet;
import org.junit.Test;
import org.mp.naumann.algorithms.fd.incremental.CompressedDiff;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DeletePrunerTest {

    @Test
    public void test() {
        int numAttributes = 4;
        DeletePruner deletePruner = new DeletePruner(numAttributes, HashSet::new);
        OpenBitSet agreeSet = new OpenBitSet(numAttributes);
        agreeSet.fastSet(0);
        agreeSet.fastSet(1);
        agreeSet.fastSet(2);
        deletePruner.addAgreeSet(agreeSet, 0, 1);
        deletePruner.addAgreeSet(agreeSet, 0, 2);
        OpenBitSet toCheck = new OpenBitSet(numAttributes);
        toCheck.fastSet(0);
        toCheck.fastSet(1);
        ValidationPruner pruner = deletePruner.analyzeDiff(new CompressedDiff(new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>()));
        assertTrue(pruner.doesNotNeedValidation(toCheck, new OpenBitSet(numAttributes)));
        Map<Integer, int[]> deletes = new HashMap<>();
        deletes.put(1, null);
        pruner = deletePruner.analyzeDiff(new CompressedDiff(new HashMap<>(), deletes, new HashMap<>(), new HashMap<>()));
        assertTrue(pruner.doesNotNeedValidation(toCheck, new OpenBitSet(numAttributes)));
        deletes.put(2, null);
        pruner = deletePruner.analyzeDiff(new CompressedDiff(new HashMap<>(), deletes, new HashMap<>(), new HashMap<>()));
        assertFalse(pruner.doesNotNeedValidation(toCheck, new OpenBitSet(numAttributes)));
    }
}
