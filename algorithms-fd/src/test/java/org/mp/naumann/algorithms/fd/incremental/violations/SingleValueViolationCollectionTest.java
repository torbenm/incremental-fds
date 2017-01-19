package org.mp.naumann.algorithms.fd.incremental.violations;

import org.apache.lucene.util.OpenBitSet;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SingleValueViolationCollectionTest {

    @Test
    public void testIsMatch(){
        OpenBitSet attrs = new OpenBitSet(5);
        attrs.fastSet(1);
        attrs.fastSet(3);

        List<Integer> violating = Arrays.asList(12, 28);

        List<Integer> match = Arrays.asList(0,12,3,28,5);
        List<Integer> partialMatch = Arrays.asList(0,12,3,5,28);
        List<Integer> wrongLength = Arrays.asList(23,12);
        List<Integer> nomatch = Arrays.asList(1,2,3,4,5);

        assertTrue("Test if match is matching", SingleValueViolationCollection.isMatch(attrs, violating, match));
        assertFalse("Test if partial match is not matching", SingleValueViolationCollection.isMatch(attrs, violating, partialMatch));
        assertFalse("Test if wrong length is not matching", SingleValueViolationCollection.isMatch(attrs, violating, wrongLength));
        assertFalse("Test if no match is not matching", SingleValueViolationCollection.isMatch(attrs, violating, nomatch));
    }
}
