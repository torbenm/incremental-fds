package org.mp.naumann.algorithms.fd.incremental.violations;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.pruning.ViolatingPair;
import org.mp.naumann.algorithms.fd.incremental.violations.matcher.IntersectionMatcher;
import org.mp.naumann.algorithms.fd.incremental.violations.matcher.Matcher;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class MultipleValuesViolationCollection implements ViolationCollection {

    private final Map<OpenBitSet, ViolatingPairCollection> violationsMapById = new HashMap<>();
    private final List<OpenBitSetFD> invalidFDs = new ArrayList<>();
    private final int capacity;
    private final Matcher matcher = new IntersectionMatcher();
    private final IncrementalFDConfiguration configuration;
    private int numAttributes = 0;

    public MultipleValuesViolationCollection(IncrementalFDConfiguration configuration, int capacity) {
        this.capacity = capacity;
        this.configuration = configuration;
    }

    public MultipleValuesViolationCollection(IncrementalFDConfiguration configuration) {
        this(configuration, 5);
    }



    @Override
    public void add(OpenBitSet orgAttr, int violatingRecord1, int violatingRecord2) {
        OpenBitSet attrs = orgAttr.clone();
        if(!this.violationsMapById.containsKey(attrs)){
            this.violationsMapById.put(attrs, new ViolatingPairCollection(new HashSet<>(capacity)));
        } else {
            if(this.violationsMapById.get(attrs).size() >= capacity){
                return;
            }
        }
        this.violationsMapById.get(attrs).add(new ViolatingPair(violatingRecord1, violatingRecord2));
    }


    @Override
    public Collection<OpenBitSetFD> getAffected(FDSet negativeCoverToUpdate, Collection<Integer> removedRecords) {
        List<OpenBitSet> affected = new ArrayList<>();
        int aff = 0, skip = 0;
        for(Map.Entry<OpenBitSet, ViolatingPairCollection> entry : violationsMapById.entrySet()) {
            entry.getValue().removeAllIntersections(removedRecords);
            if(entry.getValue().size() < 1) {
                affected.add(entry.getKey());
                negativeCoverToUpdate.remove(entry.getKey());
                aff++;
            }else{
                skip++;
            }
        }
        FDLogger.log(Level.FINE, "In search for violations, matched "+ aff+ ", skipped "+ skip);
        return affected.parallelStream()
                .map(obs -> BitSetUtils.toOpenBitSetFDCollection(obs, numAttributes))
                .flatMap(Collection::stream).collect(Collectors.toList());
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
    public boolean isInvalid(OpenBitSet lhs, int rhs) {
        return violationsMapById.containsKey(lhs) && violationsMapById.get(lhs).size() > 0;
    }

    @Override
    public void setNumAttributes(int numAttributes) {
        this.numAttributes = numAttributes;
    }
}