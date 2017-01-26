package org.mp.naumann.algorithms.fd.incremental.violations;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.violations.matcher.IntersectionMatcher;
import org.mp.naumann.algorithms.fd.incremental.violations.matcher.Matcher;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SingleValueViolationCollection implements ViolationCollection {

    private final Map<OpenBitSet, int[]> violationsMap = new HashMap<>();
    private final Map<OpenBitSet, Integer> violationMapById = new HashMap<>();
    private final List<OpenBitSetFD> invalidFDs = new ArrayList<>();
    private final Matcher matcher = new IntersectionMatcher();
    private final IncrementalFDConfiguration configuration;

    public SingleValueViolationCollection(IncrementalFDConfiguration configuration) {
        this.configuration = configuration;
        throw new IllegalArgumentException("DO NOT USE! Just left here for documentation and maybe further reuse.");
    }


    @Override
    public void add(OpenBitSet attr, int violatingRecord) {
        this.violationMapById.put(attr.clone(), violatingRecord);
    }


    @Override
    public List<OpenBitSet> getAffected(FDSet negativeCoverToUpdate, Collection<Integer> removedRecords) {
        List<OpenBitSet> affected = new ArrayList<>();
        for(Map.Entry<OpenBitSet, Integer> entry : this.violationMapById.entrySet()) {

            if(removedRecords.contains(entry.getValue())) {
                affected.add(entry.getKey());
                negativeCoverToUpdate.remove(entry.getKey());
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
