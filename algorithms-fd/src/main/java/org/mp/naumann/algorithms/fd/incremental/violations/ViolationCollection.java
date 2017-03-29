package org.mp.naumann.algorithms.fd.incremental.violations;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

public interface ViolationCollection extends Serializable {

    void add(OpenBitSet attr, int violatingRecord1, int violatingRecord2);

    Collection<OpenBitSetFD> getAffected(FDSet negativeCoverToUpdate, Collection<Integer> removedRecords);

    void addInvalidFd(Collection<OpenBitSetFD> fd);

    List<OpenBitSetFD> getInvalidFds();

    boolean isInvalid(OpenBitSet lhs, int rhs);

    default void setNumAttributes(int numAttributes) {
    }

    ;
}
