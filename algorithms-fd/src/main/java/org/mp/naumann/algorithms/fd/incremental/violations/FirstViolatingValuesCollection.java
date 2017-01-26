package org.mp.naumann.algorithms.fd.incremental.violations;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.violations.matcher.IntersectionMatcher;
import org.mp.naumann.algorithms.fd.incremental.violations.matcher.Matcher;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;
import org.mp.naumann.algorithms.fd.utils.PrintUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FirstViolatingValuesCollection implements ViolationCollection {

    private final Map<OpenBitSet, Set<int[]>> violationsMap = new HashMap<>();
    private final List<OpenBitSetFD> invalidFDs = new ArrayList<>();
    private final int capacity;
    private final Matcher matcher = new IntersectionMatcher();
    public FirstViolatingValuesCollection(int capacity) {
        this.capacity = capacity;
    }

    public FirstViolatingValuesCollection() {
        this(5);
    }

    @Override
    public void add(OpenBitSet orgAttrs, List<Integer> violatingValues) {
        OpenBitSet attrs = orgAttrs.clone();
        if(!this.violationsMap.containsKey(attrs)){
            this.violationsMap.put(attrs, new HashSet<>(capacity));
        } else {
            if(this.violationsMap.get(attrs).size() >= capacity){
                return;
            }
        }
        this.violationsMap.get(attrs).add(violatingValues.stream().mapToInt(r->r).toArray());
    }

    @Override
    public List<OpenBitSet> getAffected(FDSet negativeCover, Map<Integer, int[]> removedValues) {
        List<OpenBitSet> affected = new ArrayList<>();
        int aff = 0, skip = 0;
        for(Map.Entry<OpenBitSet, Set<int[]>> entry : violationsMap.entrySet()) {

            int numAffect = 0;
            OpenBitSet attrs = entry.getKey();
            for(int[] array : entry.getValue()){
                for(int[] record : removedValues.values()){
                    if(matcher.match(attrs, array, record)){
                        numAffect++;
                        break;
                    }
                }
            }

            // If only one is left it might be valid!
            if(numAffect >= entry.getValue().size()) {
          //      PrintUtils.print(numAffect, entry.getValue().size());
                affected.add(entry.getKey());
                negativeCover.remove(attrs);
                aff++;
            }else{
                skip++;
            }
        }
        PrintUtils.print(aff, skip);
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



}