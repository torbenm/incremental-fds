package org.mp.naumann.algorithms.fd.incremental.test;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public abstract class Validator {

    private int level = 0;
    int numAttributes;
    private ExecutorService executor;
    protected int numRecords;
    protected List<? extends PositionListIndex> plis;
    protected CompressedRecords compressedRecords;
    private int validations = 0;

    Validator(int numRecords, CompressedRecords compressedRecords, List<? extends PositionListIndex> plis, boolean parallel) {
        this.numRecords = numRecords;
        this.plis = plis;
        this.compressedRecords = compressedRecords;
        this.numAttributes = plis.size();

        if (parallel) {
            int numThreads = Runtime.getRuntime().availableProcessors();
            this.executor = Executors.newFixedThreadPool(numThreads);
        }
    }

    int getValidations() {
        return validations;
    }

    private class ValidationResult {
        int validations = 0;
        int intersections = 0;
        List<OpenBitSetFD> collectedFDs = new ArrayList<>();
        public List<IntegerPair> comparisonSuggestions = new ArrayList<>();
        public void add(ValidationResult other) {
            this.validations += other.validations;
            this.intersections += other.intersections;
            this.collectedFDs.addAll(other.collectedFDs);
            this.comparisonSuggestions.addAll(other.comparisonSuggestions);
        }
    }

    private class ValidationTask implements Callable<ValidationResult> {
        private LhsRhsPair elementLhsPair;
        void setElementLhsPair(LhsRhsPair elementLhsPair) {
            this.elementLhsPair = elementLhsPair;
        }
        ValidationTask(LhsRhsPair elementLhsPair) {
            this.elementLhsPair = elementLhsPair;
        }
        public ValidationResult call() throws Exception {
            ValidationResult result = new ValidationResult();

            LatticeElement element = this.elementLhsPair.getElement();
            OpenBitSet lhs = this.elementLhsPair.getLhs();
            OpenBitSet rhs = element.getRhs();

            int rhsSize = (int) rhs.cardinality();
            if (rhsSize == 0)
                return result;
            result.validations = result.validations + rhsSize;

            if (lhs.isEmpty()) {
                // Check if rhs is unique
                for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
                    if (!Validator.this.plis.get(rhsAttr).isConstant(Validator.this.numRecords)) {
                        handleInvalidRhs(result, element, lhs, rhsAttr);
                    } else {
                        handleValidRhs(result, element, lhs, rhsAttr);
                    }
                    result.intersections++;
                }
            }
            else if (lhs.cardinality() == 1) {
                // Check if lhs from plis refines rhs
                int lhsAttribute = lhs.nextSetBit(0);
                for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
                    if (!Validator.this.plis.get(lhsAttribute).refines(Validator.this.compressedRecords, rhsAttr, Validator.this.isTopDown())) {
                        handleInvalidRhs(result, element, lhs, rhsAttr);
                    } else {
                        handleValidRhs(result, element, lhs, rhsAttr);
                    }
                    result.intersections++;
                }
            }
            else {
                // Check if lhs from plis plus remaining inverted plis refines rhs
                int firstLhsAttr = lhs.nextSetBit(0);

                lhs.clear(firstLhsAttr);
                OpenBitSet validRhs = Validator.this.plis.get(firstLhsAttr).refines(Validator.this.compressedRecords, lhs, rhs, result.comparisonSuggestions, Validator.this.isTopDown());
                lhs.set(firstLhsAttr);

                OpenBitSet invalidRhs = rhs.clone();
                invalidRhs.andNot(validRhs);

                for (int rhsAttr = validRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = validRhs.nextSetBit(rhsAttr + 1)) {
                    handleValidRhs(result, element, lhs, rhsAttr);
                }

                for (int rhsAttr = invalidRhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = invalidRhs.nextSetBit(rhsAttr + 1)) {
                    handleInvalidRhs(result, element, lhs, rhsAttr);
                }

                result.intersections++;
            }
            return result;
        }

        private void handleValidRhs(ValidationResult result, LatticeElement element, OpenBitSet lhs, int rhsAttr) {
            validRhs(element, rhsAttr);
            if(collectValid()) {
                result.collectedFDs.add(new OpenBitSetFD(lhs.clone(), rhsAttr));
            }
        }

        private void handleInvalidRhs(ValidationResult result, LatticeElement element, OpenBitSet lhs, int rhsAttr) {
            invalidRhs(element, rhsAttr);
            if (collectInvalid()) {
                result.collectedFDs.add(new OpenBitSetFD(lhs.clone(), rhsAttr));
            }
        }
    }

    protected abstract boolean collectInvalid();

    protected abstract boolean collectValid();

    private ValidationResult validateSequential(Collection<LhsRhsPair> currentLevel) throws AlgorithmExecutionException {
        ValidationResult validationResult = new ValidationResult();

        ValidationTask task = new ValidationTask(null);
        for (LhsRhsPair elementLhsPair : currentLevel) {
            task.setElementLhsPair(elementLhsPair);
            try {
                validationResult.add(task.call());
            }
            catch (Exception e) {
                e.printStackTrace();
                throw new AlgorithmExecutionException(e.getMessage());
            }
        }

        return validationResult;
    }

    private ValidationResult validateParallel(Collection<LhsRhsPair> currentLevel) throws AlgorithmExecutionException {
        ValidationResult validationResult = new ValidationResult();

        List<Future<ValidationResult>> futures = new ArrayList<>();
        for (LhsRhsPair elementLhsPair : currentLevel) {
            ValidationTask task = new ValidationTask(elementLhsPair);
            futures.add(this.executor.submit(task));
        }

        for (Future<ValidationResult> future : futures) {
            try {
                validationResult.add(future.get());
            }
            catch (ExecutionException | InterruptedException e) {
                this.executor.shutdownNow();
                e.printStackTrace();
                throw new AlgorithmExecutionException(e.getMessage());
            }
        }

        return validationResult;
    }

    protected abstract boolean isTopDown();

    List<IntegerPair> validate() throws AlgorithmExecutionException {
        return validateLattice(getLattice(), getInverseLattice());
    }

    protected abstract Lattice getLattice();

    protected abstract Lattice getInverseLattice();

    private List<IntegerPair> validateLattice(Lattice lattice, Lattice inverseLattice) throws AlgorithmExecutionException {
        List<IntegerPair> comparisonSuggestions = new ArrayList<>();
        int previousNumInvalidFds = 0;
        while (level <= lattice.getDepth()) {
            Collection<LhsRhsPair> currentLevel = lattice.getLevel(level);
            if(!isTopDown()) {
                currentLevel.forEach(pair -> pair.getLhs().flip(0, numAttributes));
            }
            ValidationResult result = validate(currentLevel);
            validations += result.validations;
            comparisonSuggestions.addAll(result.comparisonSuggestions);
            int candidates = 0;
            for (OpenBitSetFD fd : result.collectedFDs) {
                if(!isTopDown()) {
                    fd.getLhs().flip(0, numAttributes);
                }
                OpenBitSetFD flippedFd = flip(fd);
                inverseLattice.addFunctionalDependency(flippedFd);
                inverseLattice.removeSpecializations(flippedFd);
                List<OpenBitSetFD> specializations = generateSpecializations(fd);
                candidates += specializations.size();
                for (OpenBitSetFD specialization : specializations) {
                    lattice.addFunctionalDependency(specialization);
                }
            }
            int numInvalidFds = result.collectedFDs.size();
            int numValidFds = result.validations - numInvalidFds;
            FDLogger.log(Level.FINER, result.intersections + " intersections; " + result.validations + " validations; " + numInvalidFds + " invalid; " + candidates + " new candidates; --> " + numValidFds + " FDs");

            // Decide if we continue validating the next level or if we go back into the sampling phase
            if (switchToSampler(previousNumInvalidFds, numInvalidFds, numValidFds)) {
                return comparisonSuggestions;
            }
            previousNumInvalidFds = numInvalidFds;
            level++;
        }

        if (this.executor != null) {
            this.executor.shutdown();
            try {
                this.executor.awaitTermination(365, TimeUnit.DAYS);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    protected abstract boolean switchToSampler(int previousNumInvalidFds, int numInvalidFds, int numValidFds);

    protected abstract List<OpenBitSetFD> generateSpecializations(OpenBitSetFD fd);

    private OpenBitSetFD flip(OpenBitSetFD fd) {
        OpenBitSet lhs = fd.getLhs().clone();
        lhs.flip(0, numAttributes);
        return new OpenBitSetFD(lhs, fd.getRhs());
    }

    private ValidationResult validate(Collection<LhsRhsPair> currentLevel) throws AlgorithmExecutionException {
        return (this.executor == null) ? this.validateSequential(currentLevel) : this.validateParallel(currentLevel);
    }

    protected abstract void validRhs(LatticeElement elem, int rhs);
    protected abstract void invalidRhs(LatticeElement elem, int rhs);

}
