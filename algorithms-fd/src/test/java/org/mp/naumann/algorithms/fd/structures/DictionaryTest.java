package org.mp.naumann.algorithms.fd.structures;

import org.junit.Test;
import org.mp.naumann.algorithms.fd.utils.PliUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DictionaryTest {

    @Test
    public void testInsertion() {
        Dictionary<Integer> dictionary = new Dictionary<>();
        int val = dictionary.getOrAdd(1);
        assertEquals(val, dictionary.getOrAdd(1));
        assertNotEquals(val, dictionary.getOrAdd(2));
    }

    @Test
    public void testNullEqualsNull() {
        Dictionary<Integer> dictionary = new Dictionary<>(true);
        int val = dictionary.getOrAdd(null);
        assertNotEquals(val, PliUtils.UNIQUE_VALUE);
    }

    @Test
    public void testNullDoesNotEqualsNull() {
        Dictionary<Integer> dictionary = new Dictionary<>(false);
        int val = dictionary.getOrAdd(null);
        assertEquals(val, PliUtils.UNIQUE_VALUE);
    }

}
