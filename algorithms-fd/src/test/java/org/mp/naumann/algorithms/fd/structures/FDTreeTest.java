package org.mp.naumann.algorithms.fd.structures;

import org.apache.lucene.util.OpenBitSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mp.naumann.algorithms.fd.structures.FDTree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FDTreeTest {

    private FDTree fdtree;

    @Before
    public void setUp() throws Exception {
        this.fdtree = new FDTree(5, -1);
        OpenBitSet lhs = new OpenBitSet();
        lhs.set(0);
        lhs.set(1);
        lhs.set(3);
        this.fdtree.addFunctionalDependency(lhs, 2);
    }

    @After
    public void tearDown() throws Exception {
    }


    @Test
    public void testContainsGeneralization() {
        OpenBitSet lhs = new OpenBitSet();
        lhs.set(0);
        lhs.set(1);
        assertFalse(this.fdtree.containsFdOrGeneralization(lhs, 2));
        lhs.set(3);
        lhs.set(4);
        assertTrue(this.fdtree.containsFdOrGeneralization(lhs, 2));
    }


    @Test
    public void testGetGeneralizationAndDelete() {
        OpenBitSet lhs = new OpenBitSet();
        lhs.set(0);
        lhs.set(1);
        lhs.set(3);
        lhs.set(4);
        OpenBitSet specLhs = this.fdtree.getFdAndGeneralizations(lhs, 2).get(0);

        OpenBitSet expResult = new OpenBitSet();

        expResult.set(0);
        expResult.set(1);
        expResult.set(3);
        assertEquals(expResult, specLhs);
    }


    @Test
    public void testDeleteGeneralizations() {
        fdtree = new FDTree(4, -1);
        OpenBitSet lhs = new OpenBitSet();
        lhs.set(0);
        lhs.set(1);

        this.fdtree.addFunctionalDependency(lhs, 3);
        lhs.clear(1);
        lhs.set(2);
        this.fdtree.addFunctionalDependency(lhs, 3);

        //lhs.set(1);
        //this.fdtree.deleteGeneralizations(lhs, 3, 0);
        //assertTrue(this.fdtree.isEmpty());
    }


}

