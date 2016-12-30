package org.mp.naumann.algorithms.fd.incremental.bloom;

import com.google.common.hash.BloomFilter;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class ValueCombinationTest {

    @Test
    public void test() {
        BloomFilter<Set<ColumnValue>> filter = BloomFilter.create(new ValueCombinationFunnel(), 100_000);
        Set<ColumnValue> s = new HashSet<>();
        s.add(new ColumnValue("c1", "1"));
        s.add(new ColumnValue("c2", "3"));
        filter.put(s);
        Set<ColumnValue> s2 = new HashSet<>();
        s2.add(new ColumnValue("c1", "1"));
        s2.add(new ColumnValue("c2", "3"));
        assertTrue(filter.mightContain(s2));
    }
}
