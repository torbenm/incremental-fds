package org.mp.naumann.algorithms.fd.utils;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class PowerSetTest {
	
	@Test
	public void test() {
		Set<Integer> originalSet = new HashSet<>(Arrays.asList(1, 2, 3, 4));
		assertEquals(1, PowerSet.getPowerSet(originalSet, 0).size());
		assertEquals(5, PowerSet.getPowerSet(originalSet, 1).size());
		assertEquals(11, PowerSet.getPowerSet(originalSet, 2).size());
		assertEquals(15, PowerSet.getPowerSet(originalSet, 3).size());
		assertEquals(16, PowerSet.getPowerSet(originalSet, 4).size());
	}

}
