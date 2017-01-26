package org.mp.naumann.algorithms.fd.structures;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class DictionaryTest {

    @Test
    public void testInsertion() {
        Dictionary<Integer> dictionary = new Dictionary<>();
        int val1 = dictionary.getOrAdd(1);
        assertEquals(val1, dictionary.getOrAdd(1).intValue());
        assertNotEquals(val1, dictionary.getOrAdd(2).intValue());
    }

    @Test
    public void testNullEqualsNull() {
        Dictionary<Integer> dictionary = new Dictionary<>(true);
        Integer val1 = dictionary.getOrAdd(null);
        assertNotNull(val1);
        assertEquals(val1, dictionary.getOrAdd(null));
    }

    @Test
    public void testNullDoesNotEqualsNull() {
        Dictionary<Integer> dictionary = new Dictionary<>(false);
        Integer val1 = dictionary.getOrAdd(null);
        assertNull(val1);
    }

}
