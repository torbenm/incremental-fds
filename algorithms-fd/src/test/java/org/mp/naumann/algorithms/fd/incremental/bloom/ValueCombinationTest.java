package org.mp.naumann.algorithms.fd.incremental.bloom;

import com.google.common.hash.BloomFilter;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class ValueCombinationTest {

    @Test
    public void test() {
        BloomFilter<List<ColumnValue>> filter = BloomFilter.create(new ValueCombinationFunnel(), 100_000);
        List<ColumnValue> s = Arrays.asList(new ColumnValue(1, "1"), new ColumnValue(2, "3"));
        filter.put(s);
        List<ColumnValue> s2 = Arrays.asList(new ColumnValue(1, "1"), new ColumnValue(2, "3"));
        assertTrue(filter.mightContain(s2));
    }
}
