package org.mp.naumann.algorithms.fd.incremental;

import org.apache.lucene.util.OpenBitSet;

public class BitSetUtils {

	public static boolean isContained(OpenBitSet a, OpenBitSet b) {
		return OpenBitSet.andNotCount(a, b) == 0;
	}
	
	public static String toString(OpenBitSet a) {
		try {
            StringBuilder s = new StringBuilder();

            for (int i = 0; i < a.length(); i++) {
                System.out.print(i);
                s.append(a.fastGet(i) ? 1 : 0);
            }
            return s.toString();
        }catch(AssertionError e){
            return "";
        }
	}

}
