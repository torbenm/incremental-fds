package org.mp.naumann.algorithms.fd.hyfd.validation;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.FDTreeElement;
import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;
import org.mp.naumann.algorithms.fd.structures.plis.PliCollection;

import java.util.concurrent.Callable;
import java.util.function.Function;

class ValidationTask implements Callable<ValidationResult> {

    private final PliCollection plis;
    private final int level;
    private FDTreeElementLhsPair elementLhsPair;
   
    ValidationTask(PliCollection plis, int level, FDTreeElementLhsPair elementLhsPair) {
        this.plis = plis;
        this.level = level;
        this.elementLhsPair = elementLhsPair;
    }

    void setElementLhsPair(FDTreeElementLhsPair elementLhsPair) {
        this.elementLhsPair = elementLhsPair;
    }

    public ValidationResult call() throws Exception {

        ValidationResult result = new ValidationResult();
        FDTreeElement element = this.elementLhsPair.getElement();
        OpenBitSet lhs = this.elementLhsPair.getLhs();
        OpenBitSet rhs = element.getFds();
        int rhsSize = (int) rhs.cardinality();

        if (rhsSize == 0) return result;

        result.incrementValidations(rhsSize);

        switch(level){
            case 0:
                filterNonUniqueRhs(result, element, rhs, lhs);
                break;
            case 1:
                filterNonRefiningLhs(result, element, rhs, lhs);
                break;
            default:
                checkIfLhsPlusRemainingInvertedPlisRefinesRhs(result, element, rhs, lhs);
        }
        return result;
    }

    private void filterNonUniqueRhs(ValidationResult result, FDTreeElement element,
                                    OpenBitSet rhs, OpenBitSet lhs){
        // Check if rhs is unique
        filterFds(result, element, rhs, lhs, this::isRhsUnique);
    }

    private void filterNonRefiningLhs(ValidationResult result, FDTreeElement element,
                                      OpenBitSet rhs, OpenBitSet lhs){

        // Check if lhs from plis refines rhs
        final int lhsAttr = lhs.nextSetBit(0);
        filterFds(result, element, rhs, lhs, rhsAttr -> doesLhsRefineRhs(lhsAttr, rhsAttr));
    }

    private void filterFds(ValidationResult result, FDTreeElement element,
                           OpenBitSet rhs, OpenBitSet lhs,
                           Function<Integer, Boolean> filter){
        for(int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)){
            if(!filter.apply(rhsAttr)){
                element.removeFd(rhsAttr);
                result.addInvalidFd(lhs, rhsAttr);
            }
            result.incrementIntersections();
        }
    }

    private boolean isRhsUnique(int rhsAttr){
        return this.plis.get(rhsAttr).isConstant(this.plis.getNumberOfLastRecords());
    }
    private boolean doesLhsRefineRhs(int lhsAttr, int rhsAttr){
        return this.plis.get(lhsAttr).refines(this.plis.getCompressed(), rhsAttr);
    }

    private void checkIfLhsPlusRemainingInvertedPlisRefinesRhs(ValidationResult result, FDTreeElement element,
                                                               OpenBitSet lhs, OpenBitSet rhs){
        // Check if lhs from plis plus remaining inverted plis refines rhs
        int firstLhsAttr = lhs.nextSetBit(0);

        lhs.clear(firstLhsAttr);
        OpenBitSet validRhs = this.plis.get(firstLhsAttr)
                .refines(this.plis.getCompressed(), lhs, rhs, result.getComparisonSuggestions());
        lhs.set(firstLhsAttr);

        result.incrementIntersections();

        rhs.andNot(validRhs); // Now contains all invalid FDs
        element.setFds(validRhs); // Sets the valid FDs in the OpenBitFunctionalDependency tree

        for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1))
            result.addInvalidFd(lhs, rhsAttr);
    }
}