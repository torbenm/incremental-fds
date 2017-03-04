package org.mp.naumann.algorithms.fd.incremental.deprecated.validator;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.deprecated.MemoryGuardian;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.incremental.violations.ViolationCollection;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.FDTreeElement;
import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.utils.FDTreeUtils;

import java.util.List;

@Deprecated
public class GeneralizingValidator extends Validator<Boolean> {

    private final ViolationCollection violationCollection;

    public GeneralizingValidator(IncrementalFDConfiguration configuration, FDSet negCover,
                                 FDTree posCover, int numRecords, CompressedRecords compressedRecords,
                                 List<? extends PositionListIndex> plis, float efficiencyThreshold,
                                 boolean parallel, MemoryGuardian memoryGuardian, ViolationCollection violationCollection) {
        super(configuration, posCover, numRecords, compressedRecords, plis, efficiencyThreshold, parallel, memoryGuardian, negCover);
        findValid = true;
        this.violationCollection = violationCollection;
    }

    @Override
    protected boolean isTopDown() {
        return false;
    }

    @Override
    protected boolean checkLevelBounds() {
        return level >= 0;
    }

    @Override
    protected void setInitialLevel() {
        level = posCover.getDepth();
    }

    @Override
    protected List<FDTreeElementLhsPair> getInitialLevel() {
        List<FDTreeElementLhsPair> initialLevel = FDTreeUtils.getFdLevel(posCover, level);
        // Zoom to first level that actually has some content
        while(initialLevel.size() == 0 && level > 0){
            level--;
            initialLevel = FDTreeUtils.getFdLevel(posCover, level);
        }
        return initialLevel;
    }

    @Override
    protected int generateNextLevel(ValidationResult validationResult) {

        // If we have found a valid FD, this also means that the specialisation of this FD is not minimal
        // Thus we must remove all from the previous level
        clearPreviousLevel(validationResult.validFDs);

        if(configuration.usingPruneGeneralizations())
            removeGeneralizations(validationResult.invalidFDs);

        // Generate new FDs from the invalid FDs and add them to the next level as well
        // In contrast to the "Normal" IncrementalValidator, as we go bottom up, we create the next level out of the valid FDs
        int candidates = generateNextLevel(validationResult.validFDs);

        this.level--;
        return candidates;
    }


    @Override
    protected Boolean switchToSampler(List<IntegerPair> comparisonSuggestions) {
        return true;
    }

    @Override
    protected Boolean terminate(List<IntegerPair> comparisonSuggestions) {
        return false;
    }

    @Override
    protected boolean reachedMaxDepth(ValidationResult validationResult) {
        return false;
    }

    private void clearPreviousLevel(List<FD> validFDs){
        validFDs.forEach(this::clearPreviousLevel);
    }

    private void clearPreviousLevel(FD validFD){
	    if(validFD.lhs.cardinality() == 0)
	        return;
	    int previousLevel = this.level + 1; // Get this from the FD maybe instead?
        posCover.getLevel(previousLevel, validFD.lhs).forEach(e -> e.getElement().removeFd(validFD.rhs));
    }


    private int generateNextLevel(List<FD> validFDs){
        return validFDs.stream().filter(this::couldBeValid).mapToInt(this::generateNextLevel).sum();
    }

    private boolean couldBeValid(FD fd) {
        return !violationCollection.isInvalid(fd.lhs, fd.rhs);
    }

    private int removeGeneralizations(List<FD> invalidFDs) {
        // As each invalid FD, such as ADB -/-> C means we don't have to look at
        // A -> C, B -> C, D -> C, AD -> C, AB -> C, DB -> C,
        // we should drop all possible generalizations.
        return invalidFDs.stream().mapToInt(fd -> this.posCover.removeFdAndGeneralizations(fd.lhs, fd.rhs)).sum();
    }

    private int generateNextLevel(FD validFD){
        if(validFD.lhs.cardinality() == 1)
            return 0;
        int candidates = 0;
        for(int removeLhs = validFD.lhs.nextSetBit(0); removeLhs >= 0; removeLhs =validFD.lhs.nextSetBit(removeLhs+1)){
            OpenBitSet lhs = validFD.lhs.clone();
            lhs.clear(removeLhs);
            candidates += addFunctionalDependency(lhs, validFD.rhs);
        }
        return candidates;
    }

    private int addFunctionalDependency(OpenBitSet lhs, int rhs){
        int candidates = 0;
        // Check if this attribute has already been added
        FDTreeElement child = this.posCover.addFunctionalDependencyGetIfNew(lhs, rhs);
        if(child != null) {
            candidates++;
        }
        //TODO: Make Memory Guardian cut the bottom, not the top
        //checkMemoryGuardian();
        return candidates;
    }



}
