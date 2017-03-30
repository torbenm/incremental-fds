package org.mp.naumann.algorithms.fd.incremental;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.incremental.IncrementalValidator.ValidationResult;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

import java.util.List;

class ActualValidator {

    private final List<? extends PositionListIndex> plis;
    private final int numRecords;
    private final ValidationCallback validCallback;
    private final ValidationCallback invalidCallback;
    private final CompressedRecords compressedRecords;
    private final boolean validateAll;

    ActualValidator(List<? extends PositionListIndex> plis,
                    CompressedRecords compressedRecords, int numRecords,
                    ValidationCallback validCallback, ValidationCallback invalidCallback, boolean validateAll) {
        this.plis = plis;
        this.numRecords = numRecords;
        this.validCallback = validCallback;
        this.invalidCallback = invalidCallback;
        this.compressedRecords = compressedRecords;
        this.validateAll = validateAll;
    }

    ValidationResult validate(OpenBitSet lhs, int rhs) {
        OpenBitSet rhsBits = new OpenBitSet(compressedRecords.getNumAttributes());
        rhsBits.fastSet(rhs);
        return validate(lhs, rhsBits);
    }

    ValidationResult validate(OpenBitSet lhs, OpenBitSet rhs) {
        ValidationResult result = new ValidationResult();
        int rhsSize = (int) rhs.cardinality();
        if (rhsSize == 0) {
            return result;
        }
        result.validations = result.validations + rhsSize;

        if (lhs.isEmpty()) {
            // Check if rhs is unique
            for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
                if (!plis.get(rhsAttr).isConstant(numRecords)) {
                    invalidCallback.callback(lhs, rhsAttr, result.collectedFDs);
                } else {
                    validCallback.callback(lhs, rhsAttr, result.collectedFDs);
                }
                result.intersections++;
            }
        } else if (lhs.cardinality() == 1) {
            // Check if lhs from plis refines rhs
            int lhsAttribute = lhs.nextSetBit(0);
            for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
                if (!plis.get(lhsAttribute).refines(compressedRecords, rhsAttr, !validateAll)) {
                    invalidCallback.callback(lhs, rhsAttr, result.collectedFDs);
                } else {
                    validCallback.callback(lhs, rhsAttr, result.collectedFDs);
                }
                result.intersections++;
            }
        } else {
            // Check if lhs from plis plus remaining inverted plis refines rhs
            int firstLhsAttr = lhs.nextSetBit(0);

            lhs.fastClear(firstLhsAttr);
            OpenBitSet validRhs = plis.get(firstLhsAttr).refines(compressedRecords, lhs, rhs, result.comparisonSuggestions, !validateAll);
            lhs.fastSet(firstLhsAttr);

            OpenBitSet invalidRhs = rhs.clone();
            invalidRhs.andNot(validRhs);

            for (int rhsAttr = validRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = validRhs.nextSetBit(rhsAttr + 1)) {
                validCallback.callback(lhs, rhsAttr, result.collectedFDs);
            }

            for (int rhsAttr = invalidRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = invalidRhs.nextSetBit(rhsAttr + 1)) {
                invalidCallback.callback(lhs, rhsAttr, result.collectedFDs);
            }

            result.intersections++;
        }
        return result;
    }

    public interface ValidationCallback {
        void callback(OpenBitSet lhs, int rhs, List<OpenBitSetFD> collectedFDs);
    }

}
