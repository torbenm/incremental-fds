package org.mp.naumann.algorithms.fd.hyfd.validation;

import org.mp.naumann.algorithms.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;
import org.mp.naumann.algorithms.fd.structures.plis.PliCollection;
import org.mp.naumann.algorithms.fd.utils.MemoryGuardian;

import java.util.List;

public class SequentialValidator extends Validator {
    SequentialValidator(FDSet negCover, FDTree posCover, PliCollection plis, float efficiencyThreshold, MemoryGuardian memoryGuardian) {
        super(negCover, posCover, plis, efficiencyThreshold, memoryGuardian);
    }

    @Override
    protected ValidationResult validate(List<FDTreeElementLhsPair> currentLevel, int level) throws AlgorithmExecutionException {
        ValidationResult validationResult = new ValidationResult();

        ValidationTask task = new ValidationTask(getPlis(), level, null);
        for (FDTreeElementLhsPair elementLhsPair : currentLevel) {
            task.setElementLhsPair(elementLhsPair);
            try {
                validationResult.add(task.call());
            } catch (Exception e) {
                e.printStackTrace();
                throw new AlgorithmExecutionException(e.getMessage());
            }
        }

        return validationResult;
    }
}
