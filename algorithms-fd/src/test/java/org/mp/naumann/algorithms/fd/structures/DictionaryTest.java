package org.mp.naumann.algorithms.fd.structures;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DictionaryTest {

    @Test
    public void testInsertion() {
        Dictionary<Integer> dictionary = new Dictionary<>();
        int val1 = dictionary.getOrAdd(1);
        assertEquals(val1, dictionary.getOrAdd(1));
        assertNotEquals(val1, dictionary.getOrAdd(2));
    }

    @Test
    public void testNullEqualsNull() {
        Dictionary<Integer> dictionary = new Dictionary<>();
        int val1 = dictionary.getOrAdd(null);
        assertEquals(val1, dictionary.getOrAdd(null));
    }

}
