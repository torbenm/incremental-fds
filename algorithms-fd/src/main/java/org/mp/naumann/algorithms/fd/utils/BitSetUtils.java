package org.mp.naumann.algorithms.fd.utils;

import org.apache.lucene.util.OpenBitSet;

import java.util.ArrayList;
import java.util.List;

public class BitSetUtils {

	public static boolean isContained(OpenBitSet a, OpenBitSet b) {
		return OpenBitSet.andNotCount(a, b) == 0;
	}
	
	public static String toString(OpenBitSet a) {
		try {
            StringBuilder s = new StringBuilder();

            for (int i = 0; i < a.length(); i++) {
                s.append(a.fastGet(i) ? 1 : 0);
            }
            return s.toString();
        }catch(AssertionError e){
            return "";
        }
	}

	public static List<Integer> collectSetBits(OpenBitSet bitSet) {
		List<Integer> cols = new ArrayList<>();
		for (int nextSetBit = bitSet.nextSetBit(0); nextSetBit >= 0; nextSetBit = bitSet.nextSetBit(nextSetBit + 1)) {
			cols.add(nextSetBit);
		}
		return cols;
	}

	public static OpenBitSet generateOpenBitSet(int... setBits){
	    OpenBitSet obs = new OpenBitSet();
	    for(int setBit : setBits){
	        obs.set(setBit);
        }
        return obs;
    }

}
