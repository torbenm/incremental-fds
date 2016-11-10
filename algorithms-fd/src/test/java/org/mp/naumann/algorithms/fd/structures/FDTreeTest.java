package org.mp.naumann.algorithms.fd.structures;

import org.junit.BeforeClass;

public class FDTreeTest {

    private static FDTree fdTree;

    @BeforeClass
    public static void initialize(){
        fdTree = new FDTree(10, 10);
    }



}
