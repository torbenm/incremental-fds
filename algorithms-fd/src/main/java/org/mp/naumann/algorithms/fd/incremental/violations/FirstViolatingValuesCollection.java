package org.mp.naumann.algorithms.fd.incremental.violations;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.violations.matcher.IntersectionMatcher;
import org.mp.naumann.algorithms.fd.incremental.violations.matcher.Matcher;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

public class FirstViolatingValuesCollection implements ViolationCollection {

    private final Map<OpenBitSet, Set<Integer>> violationsMapById = new HashMap<>();
    private final List<OpenBitSetFD> invalidFDs = new ArrayList<>();
    private final int capacity;
    private final Matcher matcher = new IntersectionMatcher();
    private final IncrementalFDConfiguration configuration;

    public FirstViolatingValuesCollection( IncrementalFDConfiguration configuration, int capacity) {
        this.capacity = capacity;
        this.configuration = configuration;
    }

    public FirstViolatingValuesCollection(IncrementalFDConfiguration configuration) {
        this(configuration, 5);
    }



    @Override
    public void add(OpenBitSet orgAttr, int violatingRecord) {
        OpenBitSet attrs = orgAttr.clone();
        if(!this.violationsMapById.containsKey(attrs)){
            this.violationsMapById.put(attrs, new HashSet<>(capacity));
        } else {
            if(this.violationsMapById.get(attrs).size() >= capacity){
                return;
            }
        }
        this.violationsMapById.get(attrs).add(violatingRecord);
    }


    @Override
    public List<OpenBitSet> getAffected(FDSet negativeCoverToUpdate, Collection<Integer> removedRecords) {
        List<OpenBitSet> affected = new ArrayList<>();
        int aff = 0, skip = 0;
        for(Map.Entry<OpenBitSet, Set<Integer>> entry : violationsMapById.entrySet()) {
            if(matcher.match(entry.getValue(), removedRecords)) {
                affected.add(entry.getKey());
                negativeCoverToUpdate.remove(entry.getKey());
                aff++;
            }else{
                skip++;
            }
        }
        FDLogger.log(Level.FINE, "In search for violations, matched "+ aff+ ", skipped "+ skip);
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