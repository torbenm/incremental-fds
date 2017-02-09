package org.mp.naumann.algorithms.fd.incremental.violations;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.violations.tree.ViolationTreeElement;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TreeViolationCollection implements ViolationCollection {

    private final ViolationTreeElement rootElement;
    private final List<OpenBitSetFD> invalidFDs = new ArrayList<>();
    private int numAttributes;
    private final IncrementalFDConfiguration configuration;

    public TreeViolationCollection(IncrementalFDConfiguration configuration) {
        this.configuration = configuration;
        this.rootElement = new ViolationTreeElement(new OpenBitSet(), -1, configuration.getViolationCollectionSize());
    }

    @Override
    public void add(OpenBitSet attr, int violatingRecord1, int violatingRecord2) {
        OpenBitSet flipped = attr.clone();
        flipped.flip(0, numAttributes);
        rootElement.add(flipped, new ViolatingPair(violatingRecord1, violatingRecord2), flipped.nextSetBit(0));
    }

    @Override
    public Collection<OpenBitSetFD> getAffected(FDSet negativeCoverToUpdate, Collection<Integer> removedRecords) {
        Collection<OpenBitSetFD> affected = new ArrayList<>();
        this.rootElement.findAffected(removedRecords,
                BitSetUtils.generateAllOnesBitSet(numAttributes), affected);

        affected.forEach(ofd -> negativeCoverToUpdate.remove(ofd.getLhs()));
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
    public boolean isInvalid(OpenBitSet lhs, int rhs) {
        return false;
    }

    @Override
    public void setNumAttributes(int numAttributes) {
        this.rootElement.setNumAttributes(numAttributes);
        this.numAttributes = numAttributes;
    }
}
