package org.mp.naumann.algorithms.fd.structures;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.List;
import org.apache.lucene.util.OpenBitSet;
import org.junit.Test;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;

public class LatticeTest {

	@Test
	public void testAdd() {
		Lattice lattice = new Lattice(5);
		lattice.addFunctionalDependency(BitSetUtils.fromString("10000"), 0);
		lattice.addFunctionalDependency(BitSetUtils.fromString("11000"), 4);
		lattice.addFunctionalDependency(BitSetUtils.fromString("10100"), 4);
		lattice.addFunctionalDependency(BitSetUtils.fromString("10100"), 3);
		lattice.addFunctionalDependency(BitSetUtils.fromString("01011"), 2);
		lattice.addFunctionalDependency(BitSetUtils.fromString("01100"), 2);
		List<OpenBitSetFD> fds = lattice.getFunctionalDependencies();
		assertEquals(6, fds.size());
		assertTrue(fds.contains(new OpenBitSetFD(BitSetUtils.fromString("10000"), 0)));
		assertTrue(fds.contains(new OpenBitSetFD(BitSetUtils.fromString("11000"), 4)));
		assertTrue(fds.contains(new OpenBitSetFD(BitSetUtils.fromString("10100"), 4)));
		assertTrue(fds.contains(new OpenBitSetFD(BitSetUtils.fromString("10100"), 3)));
		assertTrue(fds.contains(new OpenBitSetFD(BitSetUtils.fromString("01011"), 2)));
		assertTrue(fds.contains(new OpenBitSetFD(BitSetUtils.fromString("01100"), 2)));
		Collection<LatticeElementLhsPair> level = lattice.getLevel(0);
		assertTrue(level.isEmpty());
		level = lattice.getLevel(1);
		assertEquals(1, level.size());
		level = lattice.getLevel(2);
		assertEquals(3, level.size());
		level = lattice.getLevel(3);
		assertEquals(1, level.size());
		level = lattice.getLevel(4);
		assertTrue(level.isEmpty());
		level = lattice.getLevel(5);
		assertTrue(level.isEmpty());
	}

	@Test
	public void testRemove() {
		Lattice lattice = new Lattice(5);
		lattice.addFunctionalDependency(BitSetUtils.fromString("10000"), 0);
		lattice.addFunctionalDependency(BitSetUtils.fromString("11000"), 4);
		lattice.addFunctionalDependency(BitSetUtils.fromString("10100"), 4);
		lattice.addFunctionalDependency(BitSetUtils.fromString("10100"), 3);
		lattice.addFunctionalDependency(BitSetUtils.fromString("01011"), 2);
		lattice.addFunctionalDependency(BitSetUtils.fromString("01100"), 2);
		lattice.removeFunctionalDependency(BitSetUtils.fromString("10000"), 0);
		lattice.removeFunctionalDependency(BitSetUtils.fromString("10100"), 4);
		lattice.removeFunctionalDependency(BitSetUtils.fromString("01001"), 2);
		List<OpenBitSetFD> fds = lattice.getFunctionalDependencies();
		assertEquals(4, fds.size());
		assertTrue(fds.contains(new OpenBitSetFD(BitSetUtils.fromString("11000"), 4)));
		assertTrue(fds.contains(new OpenBitSetFD(BitSetUtils.fromString("10100"), 3)));
		assertTrue(fds.contains(new OpenBitSetFD(BitSetUtils.fromString("01011"), 2)));
		assertTrue(fds.contains(new OpenBitSetFD(BitSetUtils.fromString("01100"), 2)));
		Collection<LatticeElementLhsPair> level = lattice.getLevel(0);
		assertTrue(level.isEmpty());
		level = lattice.getLevel(1);
		assertTrue(level.isEmpty());
		level = lattice.getLevel(2);
		assertEquals(3, level.size());
		level = lattice.getLevel(3);
		assertEquals(1, level.size());
		level = lattice.getLevel(4);
		assertTrue(level.isEmpty());
		level = lattice.getLevel(5);
		assertTrue(level.isEmpty());
	}

	@Test
	public void testContainsFdOrGeneralization() {
		Lattice lattice = new Lattice(5);
		lattice.addFunctionalDependency(BitSetUtils.fromString("10000"), 0);
		lattice.addFunctionalDependency(BitSetUtils.fromString("11000"), 4);
		lattice.addFunctionalDependency(BitSetUtils.fromString("10100"), 4);
		lattice.addFunctionalDependency(BitSetUtils.fromString("10100"), 3);
		lattice.addFunctionalDependency(BitSetUtils.fromString("01011"), 2);
		lattice.addFunctionalDependency(BitSetUtils.fromString("01100"), 2);
		assertTrue(lattice.containsFdOrGeneralization(BitSetUtils.fromString("10000"), 0));
		assertTrue(lattice.containsFdOrGeneralization(BitSetUtils.fromString("11000"), 4));
		assertTrue(lattice.containsFdOrGeneralization(BitSetUtils.fromString("10100"), 4));
		assertTrue(lattice.containsFdOrGeneralization(BitSetUtils.fromString("10100"), 3));
		assertTrue(lattice.containsFdOrGeneralization(BitSetUtils.fromString("01011"), 2));
		assertTrue(lattice.containsFdOrGeneralization(BitSetUtils.fromString("01100"), 2));
		assertTrue(lattice.containsFdOrGeneralization(BitSetUtils.fromString("11000"), 0));
		assertTrue(lattice.containsFdOrGeneralization(BitSetUtils.fromString("11001"), 4));
		assertTrue(lattice.containsFdOrGeneralization(BitSetUtils.fromString("10110"), 4));
		assertTrue(lattice.containsFdOrGeneralization(BitSetUtils.fromString("10101"), 3));
		assertTrue(lattice.containsFdOrGeneralization(BitSetUtils.fromString("11011"), 2));
		assertTrue(lattice.containsFdOrGeneralization(BitSetUtils.fromString("11100"), 2));
		assertFalse(lattice.containsFdOrGeneralization(BitSetUtils.fromString("00000"), 0));
		assertFalse(lattice.containsFdOrGeneralization(BitSetUtils.fromString("01000"), 4));
		assertFalse(lattice.containsFdOrGeneralization(BitSetUtils.fromString("00100"), 4));
		assertFalse(lattice.containsFdOrGeneralization(BitSetUtils.fromString("10000"), 3));
		assertFalse(lattice.containsFdOrGeneralization(BitSetUtils.fromString("01001"), 2));
		assertFalse(lattice.containsFdOrGeneralization(BitSetUtils.fromString("00100"), 2));
	}

	@Test
	public void testRemoveSpecializations() {
		Lattice lattice = new Lattice(5);
		lattice.addFunctionalDependency(BitSetUtils.fromString("10000"), 0);
		lattice.addFunctionalDependency(BitSetUtils.fromString("11000"), 4);
		lattice.addFunctionalDependency(BitSetUtils.fromString("10100"), 4);
		lattice.addFunctionalDependency(BitSetUtils.fromString("10100"), 3);
		lattice.addFunctionalDependency(BitSetUtils.fromString("01011"), 2);
		lattice.addFunctionalDependency(BitSetUtils.fromString("01100"), 2);
		lattice.removeSpecializations(BitSetUtils.fromString("01011"), 2);
		List<OpenBitSetFD> fds = lattice.getFunctionalDependencies();
		assertEquals(6, fds.size());
		lattice.removeSpecializations(BitSetUtils.fromString("01000"), 2);
		fds = lattice.getFunctionalDependencies();
		assertEquals(4, fds.size());
		Collection<LatticeElementLhsPair> level = lattice.getLevel(0);
		assertTrue(level.isEmpty());
		level = lattice.getLevel(1);
		assertEquals(1, level.size());
		level = lattice.getLevel(2);
		assertEquals(2, level.size());
		level = lattice.getLevel(3);
		assertTrue(level.isEmpty());
		level = lattice.getLevel(4);
		assertTrue(level.isEmpty());
		level = lattice.getLevel(5);
		assertTrue(level.isEmpty());
	}

	@Test
	public void testGetFdAndGeneralizations() {
		Lattice lattice = new Lattice(5);
		lattice.addFunctionalDependency(BitSetUtils.fromString("10000"), 0);
		lattice.addFunctionalDependency(BitSetUtils.fromString("11000"), 4);
		lattice.addFunctionalDependency(BitSetUtils.fromString("10100"), 4);
		lattice.addFunctionalDependency(BitSetUtils.fromString("10100"), 3);
		lattice.addFunctionalDependency(BitSetUtils.fromString("01011"), 2);
		lattice.addFunctionalDependency(BitSetUtils.fromString("01100"), 2);
		List<OpenBitSet> fds = lattice.getFdAndGeneralizations(BitSetUtils.fromString("01111"), 2);
		assertEquals(2, fds.size());
		assertTrue(fds.contains(BitSetUtils.fromString("01011")));
		assertTrue(fds.contains(BitSetUtils.fromString("01100")));
		fds = lattice.getFdAndGeneralizations(BitSetUtils.fromString("01110"), 2);
		assertEquals(1, fds.size());
		assertTrue(fds.contains(BitSetUtils.fromString("01100")));
		fds = lattice.getFdAndGeneralizations(BitSetUtils.fromString("01100"), 2);
		assertEquals(1, fds.size());
		assertTrue(fds.contains(BitSetUtils.fromString("01100")));
		fds = lattice.getFdAndGeneralizations(BitSetUtils.fromString("0100"), 2);
		assertTrue(fds.isEmpty());
	}

}
