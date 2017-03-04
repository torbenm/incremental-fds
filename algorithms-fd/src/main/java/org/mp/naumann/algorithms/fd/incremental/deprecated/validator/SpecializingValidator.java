package org.mp.naumann.algorithms.fd.incremental.deprecated.validator;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.deprecated.MemoryGuardian;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.FDTreeElement;
import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.utils.FDTreeUtils;

import java.util.List;
import java.util.logging.Level;

@Deprecated
public class SpecializingValidator extends Validator<List<IntegerPair>> {


    public SpecializingValidator(IncrementalFDConfiguration configuration, FDSet negCover, FDTree posCover, int numRecords, CompressedRecords compressedRecords, List<? extends PositionListIndex> plis, float efficiencyThreshold, boolean parallel, MemoryGuardian memoryGuardian) {
        super(configuration, posCover, numRecords, compressedRecords, plis, efficiencyThreshold, parallel, memoryGuardian, negCover);
    }

    @Override
    protected boolean isTopDown() {
        return true;
    }

    @Override
    protected boolean checkLevelBounds() {
        return level <= posCover.getDepth();
    }

    @Override
    protected void setInitialLevel() {
        level = 0;
    }

    @Override
    protected List<FDTreeElementLhsPair> getInitialLevel() {
        return FDTreeUtils.getFdLevel(posCover, level);
    }

    @Override
    protected int generateNextLevel(ValidationResult validationResult) {
        int candidates = 0;
        for (FD invalidFD : validationResult.invalidFDs) {
            for (int extensionAttr = 0; extensionAttr < this.plis.size(); extensionAttr++) {
                OpenBitSet childLhs = this.extendWith(invalidFD.lhs, invalidFD.rhs, extensionAttr);
                if (childLhs != null) {
                    FDTreeElement child = this.posCover.addFunctionalDependencyGetIfNew(childLhs, invalidFD.rhs);
                    if (child != null) {
                        candidates++;

                        this.memoryGuardian.memoryChanged(1);
                        this.memoryGuardian.match(this.negCover, this.posCover, null);
                    }
                }
            }


            if ((this.posCover.getMaxDepth() > -1) && (this.level >= this.posCover.getMaxDepth()))
                break;
        }

        this.level++;
        return candidates;
    }

    @Override
    protected List<IntegerPair> switchToSampler(List<IntegerPair> comparisonSuggestions) {
        return comparisonSuggestions;
    }

    @Override
    protected List<IntegerPair> terminate(List<IntegerPair> comparisonSuggestions) {
        return null;
    }

    @Override
    protected boolean reachedMaxDepth(ValidationResult validationResult) {
        if ((this.posCover.getMaxDepth() > -1) && (this.level >= this.posCover.getMaxDepth())) {
            int numInvalidFds = validationResult.invalidFDs.size();
            int numValidFds = validationResult.validations - numInvalidFds;
            FDLogger.log(Level.FINER, "(-)(-); " + validationResult.intersections + " intersections; " + validationResult.validations + " validations; " + numInvalidFds + " invalid; " + "-" + " new candidates; --> " + numValidFds + " FDs");
            return true;
        }
        return false;
    }


}
