package org.mp.naumann.algorithms.fd.incremental.violations;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.violations.matcher.IntersectionMatcher;
import org.mp.naumann.algorithms.fd.incremental.violations.matcher.Matcher;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SingleValueViolationCollection implements ViolationCollection {

    private final Map<OpenBitSet, ViolatingPair> violationMapById = new HashMap<>();
    private final List<OpenBitSetFD> invalidFDs = new ArrayList<>();
    private final Matcher matcher = new IntersectionMatcher();
    private final IncrementalFDConfiguration configuration;
    private int numAttributes = 0;

    public SingleValueViolationCollection(IncrementalFDConfiguration configuration) {
        this.configuration = configuration;
    }


    @Override
    public void add(OpenBitSet attr, int violatingRecord1, int violatingRecord2) {
        this.violationMapById.put(attr.clone(), new ViolatingPair(violatingRecord1, violatingRecord2));
    }


    @Override
    public Collection<OpenBitSetFD> getAffected(FDSet negativeCoverToUpdate, Collection<Integer> removedRecords) {
        List<OpenBitSet> affected = new ArrayList<>();
        for(Map.Entry<OpenBitSet, ViolatingPair> entry : this.violationMapById.entrySet()) {

            if(removedRecords.contains(entry.getValue().getFirstRecord())
                    || removedRecords.contains(entry.getValue().getSecondRecord())) {
                affected.add(entry.getKey());
                negativeCoverToUpdate.remove(entry.getKey());
            }
        }
        affected.parallelStream().forEach(violationMapById::remove);

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
        return this.violationMapById.containsKey(lhs);
    }

    @Override
    public void setNumAttributes(int numAttributes) {
        this.numAttributes = numAttributes;
    }
}
