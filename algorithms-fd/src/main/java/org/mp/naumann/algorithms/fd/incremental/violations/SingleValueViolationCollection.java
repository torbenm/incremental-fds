package org.mp.naumann.algorithms.fd.incremental.violations;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.violations.matcher.IntersectionMatcher;
import org.mp.naumann.algorithms.fd.incremental.violations.matcher.Matcher;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SingleValueViolationCollection implements ViolationCollection {

    private final Map<OpenBitSet, int[]> violationsMap = new HashMap<>();
    private final List<OpenBitSetFD> invalidFDs = new ArrayList<>();
    private final Matcher matcher = new IntersectionMatcher();

    @Override
    public void add(OpenBitSet attrs, List<Integer> violatingValues) {
        this.violationsMap.put(attrs.clone(), violatingValues.stream().mapToInt(r -> r).toArray());
    }

    @Override
    public List<OpenBitSet> getAffected(FDSet negativeCover, int[][] removedValues) {
        List<OpenBitSet> affected = new ArrayList<>();
        for(Map.Entry<OpenBitSet, int[]> entry : violationsMap.entrySet()) {
            boolean anyMatch = false;
            OpenBitSet attrs = entry.getKey();
            for(int[] record : removedValues){
                anyMatch = matcher.match(attrs, entry.getValue(), record);
                if(anyMatch) break;
            }

            if (anyMatch) {
                affected.add(entry.getKey());
                negativeCover.remove(attrs);
            }
        }
        return affected;
    }

    @Override
    public void addInvalidFd(Collection<OpenBitSetFD> fd) {
        invalidFDs.addAll(fd);
    }

    @Override
    public List<OpenBitSetFD> getInvalidFds() {
        return invalidFDs;
    }

    @Override
    public String toString(){
        StringBuilder s = new StringBuilder("Negative Cover\n");
        s.append("=========\n");
        for(Map.Entry<OpenBitSet, int[]> entry : violationsMap.entrySet()){
            s.append(BitSetUtils.toString(entry.getKey()))
                    .append(" ")
                    .append(Arrays.toString(entry.getValue()))
                    .append("\n");
        }
        s.append("=========\n");
        s.append("Invalid FDs\n");
        s.append("=========\n");
        for(OpenBitSetFD invalidFD : invalidFDs){
            s.append(BitSetUtils.toString(invalidFD.getLhs()))
                    .append(" -> ").append(invalidFD.getRhs())
                    .append("\n");
        }
        return s.toString();
    }

}
