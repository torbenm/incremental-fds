package org.mp.naumann.algorithms.fd.incremental.violations;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface ViolationCollection extends Serializable{

    void add(OpenBitSet attrs, List<Integer> violatingValues);

    List<OpenBitSet> getAffected(FDSet negativeCoverToUpdate, Map<Integer, int[]> removedValues);
    void addInvalidFd(Collection<OpenBitSetFD> fd);

    List<OpenBitSetFD> getInvalidFds();
}
