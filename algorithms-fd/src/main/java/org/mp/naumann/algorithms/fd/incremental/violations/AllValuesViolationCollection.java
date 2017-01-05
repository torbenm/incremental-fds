package org.mp.naumann.algorithms.fd.incremental.violations;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AllValuesViolationCollection implements ViolationCollection {

    private final Map<OpenBitSet, List<Set<Integer>>> invalidationsOfNegativeCoverMap = new HashMap<>();

    @Override
    public void add(OpenBitSet attrs, List<Integer> violatingValues) {
        OpenBitSet attrsCopy = attrs.clone();
        if(!this.invalidationsOfNegativeCoverMap.containsKey(attrsCopy)) {
            this.invalidationsOfNegativeCoverMap.put(attrsCopy, new ArrayList<>());
        }

        while(this.invalidationsOfNegativeCoverMap.get(attrsCopy).size() < violatingValues.size())
            this.invalidationsOfNegativeCoverMap.get(attrsCopy).add(new HashSet<>());

        for(int i = 0; i < violatingValues.size(); i++){
            this.invalidationsOfNegativeCoverMap.get(attrsCopy).get(i).add(violatingValues.get(i));
        }
    }

    @Override
    public List<OpenBitSet> getAffected(FDSet negativeCoverToUpdate, int[][] removedValues) {
        return null;
    }



    @Override
    public void addInvalidFd(Collection<OpenBitSetFD> fd) {

    }

}
