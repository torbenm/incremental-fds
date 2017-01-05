package org.mp.naumann.algorithms.fd.incremental.violations;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

public interface ViolationCollection extends Serializable{

    void add(OpenBitSet attrs, List<Integer> violatingValues);

    List<OpenBitSet> getAffected(FDSet negativeCoverToUpdate, int[][] removedValues);
    void addInvalidFd(Collection<OpenBitSetFD> fd);

}
