package org.mp.naumann.algorithms.fd.incremental.violations.tree;

import org.apache.lucene.util.OpenBitSet;
import org.junit.Test;
import org.mp.naumann.algorithms.fd.incremental.pruning.ViolatingPair;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;
import org.mp.naumann.algorithms.fd.utils.PrintUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ViolationTreeElementTest {

    private final int num_bits = 8;

    // 1111 1111
    public final OpenBitSet FULL_RHS_AVAILABLE = new OpenBitSet();

    // 1110 1001
    public final OpenBitSet RHS_AVAILABLE_AFTER_IT_1 = BitSetUtils.generateOpenBitSet(0,1,2,4,7);

    // 0110 1000
    public final OpenBitSet RHS_AVAILABLE_AFTER_IT_2 = BitSetUtils.generateOpenBitSet(1,2,4);

    // 0001 0110
    public final OpenBitSet RHS_COVER_1 = BitSetUtils.generateOpenBitSet(3,5,6);

    // 1001 0111
    public final OpenBitSet RHS_COVER_2 = BitSetUtils.generateOpenBitSet(0,3,5,6,7);

    // 0000 0000
    public final OpenBitSet RHS_COVER_3 = BitSetUtils.generateOpenBitSet();

    {
        FULL_RHS_AVAILABLE.flip(0, num_bits);
    }

    @Test
    public void test_trim_rhs(){
        ViolationTreeElement treeElement1 = new ViolationTreeElement(RHS_COVER_1, num_bits, 5);
        ViolationTreeElement treeElement2 = new ViolationTreeElement(RHS_COVER_2, num_bits, 5);
        ViolationTreeElement treeElement3 = new ViolationTreeElement(RHS_COVER_3, num_bits, 5);


        OpenBitSet rhs = FULL_RHS_AVAILABLE.clone();

        treeElement1.trimRhsWithCover(rhs);

        PrintUtils.print("Iteration 1",BitSetUtils.toString(RHS_AVAILABLE_AFTER_IT_1), BitSetUtils.toString(rhs));
        assertEquals("Iteration 1", RHS_AVAILABLE_AFTER_IT_1, rhs);

        treeElement2.trimRhsWithCover(rhs);

        PrintUtils.print("Iteration 2", BitSetUtils.toString(RHS_AVAILABLE_AFTER_IT_2), BitSetUtils.toString(rhs));
        assertEquals("Iteration 2", RHS_AVAILABLE_AFTER_IT_2, rhs);

        treeElement3.trimRhsWithCover(rhs);
        // Should stay the same as after it 2
        PrintUtils.print("Iteration 3", BitSetUtils.toString(RHS_AVAILABLE_AFTER_IT_2), BitSetUtils.toString(rhs));
        assertEquals("Iteration 3", RHS_AVAILABLE_AFTER_IT_2, rhs);
    }

    private final int NUM_ATTRIBUTES = 4;

    @Test
    public void test_simple_add(){
        ViolationTreeElement root = new ViolationTreeElement(new OpenBitSet(), NUM_ATTRIBUTES, 5);
        // 1001 is what the actual input to the 'add' method is (coming from the sampler)
        // We then have to flip it
        OpenBitSet rhsCover = BitSetUtils.generateOpenBitSet(0,3);
        rhsCover.flip(0, 4); // 0110
        assertEquals("RhsCover should be 0110", BitSetUtils.generateOpenBitSet(1,2), rhsCover);

        root.add(rhsCover, new ViolatingPair(0, 0), rhsCover.nextSetBit(0));

       ViolationTreeElement level1 =
                check_node(root, "Root", false, true, 1, 1);

       ViolationTreeElement level2 =
               check_node(level1, "L1", false, true, 2, 2);

       check_node(level2, "L2", true, false, 0);
       assertEquals("L2 should have rhsCover as cover", rhsCover, level2.getRhsCover());

        // testing now with a new rhs, being 0111
        // rhsCover = BitSetUtils.generateOpenBitSet(1,2,3);
        // root.add(rhsCover, new ViolatingPair(0, 0), rhsCover.nextSetBit(0));
    }


    @Test
    public void test_simple_add_with_preexisting(){
        ViolationTreeElement root = new ViolationTreeElement(new OpenBitSet(), NUM_ATTRIBUTES, 5);
        OpenBitSet rhsCover1 = BitSetUtils.generateOpenBitSet(1,2);
        root.add(rhsCover1, new ViolatingPair(0, 0), rhsCover1.nextSetBit(0));

        OpenBitSet rhsCover2 = BitSetUtils.generateOpenBitSet(1,2,3);
        root.add(rhsCover2, new ViolatingPair(0, 0), rhsCover2.nextSetBit(0));

        ViolationTreeElement level1 =
                check_node(root, "Root", false, true, 1, 1);

        ViolationTreeElement level2 =
                check_node(level1, "L1", false, true, 2, 2);

        ViolationTreeElement level3 = check_node(level2, "L2", true, true, 3,3 );
        assertEquals("L2 should have rhsCover1 as cover", rhsCover1, level2.getRhsCover());

        check_node(level3, "L3", true, false, 0);
        assertEquals("L3 should have rhsCover2 as cover", rhsCover2, level3.getRhsCover());
    }

    @Test
    public void test_more_complex_add_with_different_path(){
        ViolationTreeElement root = new ViolationTreeElement(new OpenBitSet(), NUM_ATTRIBUTES, 5);
        OpenBitSet rhsCover1 = BitSetUtils.generateOpenBitSet(1,2);
        root.add(rhsCover1, new ViolatingPair(0, 0), rhsCover1.nextSetBit(0));
        OpenBitSet rhsCover2 = BitSetUtils.generateOpenBitSet(1,2,3);
        root.add(rhsCover2, new ViolatingPair(0, 0), rhsCover2.nextSetBit(0));

        OpenBitSet rhsCover3 = BitSetUtils.generateOpenBitSet(0, 2); // 1010
        root.add(rhsCover3, new ViolatingPair(0, 0), rhsCover3.nextSetBit(0));

        // First follow the path from test_simple_add_with_preexisting
        ViolationTreeElement level1 =
                check_node(root, "Root", false, true, 1, 0,1);

        ViolationTreeElement level2 =
                check_node(level1, "L1", false, true, 2, 2);

        ViolationTreeElement level3 = check_node(level2, "L2", true, true, 3,3 );
        assertEquals("L2 should have rhsCover1 as cover", rhsCover1, level2.getRhsCover());

        check_node(level3, "L3", true, false, 0);
        assertEquals("L3 should have rhsCover2 as cover", rhsCover2, level3.getRhsCover());

        //Second, follow different path
        ViolationTreeElement altL1 = root.getChildren()[0];
        ViolationTreeElement altL2 =
                check_node(altL1, "ALT1", false, true, 2, 2);

        check_node(altL2, "ALT2", true, false, 2);
        assertEquals("ALT2 should have rhsCover3 as cover", rhsCover3, altL2.getRhsCover());

    }

    private ViolationTreeElement check_node(ViolationTreeElement element, String name, boolean isCover, boolean hasChildren, int returnChild, int... existingChildren){
        PrintUtils.print("Approaching "+name, element.isCover(), BitSetUtils.toString(element.getRhsCover()));

        assertEquals(name + " should "+ (isCover ? "" : "not") + " be a cover", isCover, element.isCover());
        if(hasChildren){
            assertEquals(name+" should have allocated for 4 children", NUM_ATTRIBUTES, element.getChildren().length);
            OpenBitSet existingBS = BitSetUtils.generateOpenBitSet(existingChildren);
            for(int i = 0; i < NUM_ATTRIBUTES; i++){
                if(existingBS.get(i)){
                    assertNotNull(name+" should have a child at pos "+i, element.getChildren()[i]);
                }else {
                    assertNull(name+" should have no child at pos "+i, element.getChildren()[i]);
                }
            }
            return element.getChildren()[returnChild];
        }else {
            assertNull(name+ " should have no child", element.getChildren());
            return null;
        }
    }

    @Test
    public void test_simple_find_affected_with_only_one_result(){
        ViolationTreeElement root = new ViolationTreeElement(new OpenBitSet(), NUM_ATTRIBUTES, 5);
        // 0110
        OpenBitSet rhsCover1 = BitSetUtils.generateOpenBitSet(1,2);
        root.add(rhsCover1, new ViolatingPair(0, 1), rhsCover1.nextSetBit(0));

        Collection<Integer> removedRecords = Collections.singletonList(1);
        Collection<OpenBitSetFD> affected = new ArrayList<>();
        root.findAffected(removedRecords, BitSetUtils.generateAllOnesBitSet(NUM_ATTRIBUTES), affected);

        assertEquals("Should contain two FDs", affected.size(), 2);
    }

}
