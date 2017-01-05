package org.mp.naumann.algorithms.fd.incremental.violations;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.FDSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SingleValueViolationCollection implements ViolationCollection {

    Map<OpenBitSet, List<Integer>> violationsMap = new HashMap<>();

    @Override
    public void add(OpenBitSet attrs, List<Integer> violatingValues) {
        this.violationsMap.put(attrs.clone(), violatingValues);
    }

    @Override
    public void injectIntoNegativeCover(FDSet negCoverBase) {
        for(OpenBitSet attrs : violationsMap.keySet()){
            if(!negCoverBase.contains(attrs)){
                negCoverBase.add(attrs);
            }
        }
    }

    @Override
    public void remove(List<Integer> values) {
        // This probably could be done more efficiently
        for(Map.Entry<OpenBitSet, List<Integer>> entry : violationsMap.entrySet()){
            OpenBitSet attrs = entry.getKey();
            if(isMatch(entry.getKey(), entry.getValue(), values)){
                violationsMap.remove(attrs);
            }
        }
    }

    public static boolean isMatch(OpenBitSet attrs, List<Integer> violatingValues, List<Integer> values){
        for(int i = attrs.nextSetBit(0), j = 0; i > 0; i = attrs.nextSetBit(i+1), j++){
            if(i >= values.size() || !violatingValues.get(j).equals(values.get(i))){
                return false;
            }
        }
        return true;
    }
}
