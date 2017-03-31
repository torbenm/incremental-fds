package org.mp.naumann.algorithms.fd.incremental;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.benchmark.speed.Benchmark;
import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.incremental.ActualValidator.ValidationCallback;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.incremental.pruning.ValidationPruner;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.incremental.structures.Lattice;
import org.mp.naumann.algorithms.fd.incremental.structures.LatticeElement;
import org.mp.naumann.algorithms.fd.incremental.structures.LatticeElementLhsPair;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public abstract class IncrementalValidator<T> {

    final int numAttributes;
    private final ValidatorResult validatorResult = new ValidatorResult();
    private final List<ValidationPruner> validationPruners = new ArrayList<>();
    private final int numRecords;
    private final List<? extends PositionListIndex> plis;
    private final CompressedRecords compressedRecords;
    private final float efficiencyThreshold;
    private int level = 0;
    private ExecutorService executor;

    IncrementalValidator(int numRecords, CompressedRecords compressedRecords,
                         List<? extends PositionListIndex> plis, boolean parallel, float efficiencyThreshold) {
        this.numRecords = numRecords;
        this.plis = plis;
        this.compressedRecords = compressedRecords;
        this.numAttributes = plis.size();
        this.efficiencyThreshold = efficiencyThreshold;

        if (parallel) {
            int numThreads = Runtime.getRuntime().availableProcessors();
            this.executor = Executors.newFixedThreadPool(numThreads);
        }
    }

    void addValidationPruner(ValidationPruner ValidationPruner) {
        validationPruners.add(ValidationPruner);
    }

    ValidatorResult getValidatorResult() {
        return validatorResult;
    }

    private ValidationResult validateSequential(Collection<LatticeElementLhsPair> currentLevel) throws AlgorithmExecutionException {
        ValidationResult validationResult = new ValidationResult();

        ValidationTask task = new ValidationTask(null);
        for (LatticeElementLhsPair elementLhsPair : currentLevel) {
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

    private ValidationResult validateParallel(Collection<LatticeElementLhsPair> currentLevel) throws AlgorithmExecutionException {
        ValidationResult validationResult = new ValidationResult();

        List<Future<ValidationResult>> futures = new ArrayList<>();
        for (LatticeElementLhsPair elementLhsPair : currentLevel) {
            ValidationTask task = new ValidationTask(elementLhsPair);
            futures.add(this.executor.submit(task));
        }

        for (Future<ValidationResult> future : futures) {
            try {
                validationResult.add(future.get());
            } catch (ExecutionException | InterruptedException e) {
                this.executor.shutdownNow();
                e.printStackTrace();
                throw new AlgorithmExecutionException(e.getMessage());
            }
        }

        return validationResult;
    }

    protected abstract boolean isTopDown();

    T validate() throws AlgorithmExecutionException {
        return validateLattice(getLattice(), getInverseLattice());
    }

    protected abstract Lattice getLattice();

    protected abstract Lattice getInverseLattice();

    private void pruneLevel(Collection<LatticeElementLhsPair> lvl) {
        Iterator<LatticeElementLhsPair> it = lvl.iterator();
        while (it.hasNext()) {
            LatticeElementLhsPair fd = it.next();
            if (pruneElement(fd)) {
                validatorResult.pruned += fd.getElement().getRhsFds().cardinality();
                it.remove();
            }
        }
    }

    private boolean pruneElement(LatticeElementLhsPair fd) {
        return validationPruners.stream().anyMatch(ps -> ps.doesNotNeedValidation(fd.getLhs(), fd.getElement().getRhsFds()));
    }

    private T validateLattice(Lattice lattice, Lattice inverseLattice) throws AlgorithmExecutionException {
        int previousNumInvalidFds = 0;
        while (level <= lattice.getDepth()) {
            FDLogger.log(Level.FINER, "Started validating level " + level);
            Benchmark benchmark = Benchmark.start("Validate level " + level, Benchmark.DEFAULT_LEVEL + 3);
            Collection<LatticeElementLhsPair> currentLevel = lattice.getLevel(level);
            if (!isTopDown()) {
                // lattice is neg cover and contains flipped lhs'
                currentLevel.forEach(pair -> pair.getLhs().flip(0, numAttributes));
            }
            benchmark.finishSubtask("Retrieval of " + currentLevel.size() + " candidates");
            if (!validationPruners.isEmpty()) {
                pruneLevel(currentLevel);
                benchmark.finishSubtask("Pruning");
            }
            ValidationResult result = validate(currentLevel);
            receiveResult(result);
            validatorResult.validations += result.validations;
            int candidates = 0;
            benchmark.finishSubtask("Validation of " + currentLevel.size() + " fds");
            for (OpenBitSetFD fd : result.collectedFDs) {
                OpenBitSet lhs = fd.getLhs();
                if (!isTopDown()) {
                    // flip lhs back if lattice is negCover
                    lhs.flip(0, numAttributes);
                }
                // fd changed its state, thus add it to inverse lattice
                OpenBitSet flipped = flip(lhs);
                int rhs = fd.getRhs();
                inverseLattice.addFunctionalDependency(flipped, rhs);
                // there might be generalizations in the inverse lattice
                // (specializations here because everything is flipped)
                inverseLattice.removeSpecializations(flipped, rhs);
                List<OpenBitSet> specializations = generateSpecializations(lhs, rhs);
                for (OpenBitSet specialization : specializations) {
                    if (!lattice.containsFdOrGeneralization(specialization, rhs)) {
                        candidates++;
                        lattice.addFunctionalDependency(specialization, rhs);
                    }
                }
            }
            benchmark.finishSubtask("Inducted " + candidates + " candidates from " + result.collectedFDs.size() + " fds");
            int numInvalidFds = result.collectedFDs.size();
            int numValidFds = result.validations - numInvalidFds;
            FDLogger.log(Level.FINER, result.intersections + " intersections; " + result.validations + " validations; " + numInvalidFds + " invalid; " + candidates + " new candidates; --> " + numValidFds + " FDs");

            FDLogger.log(Level.FINER, "Finished validating level " + level);
            level++;
            // Decide if we continue validating the next level or if we go back into the sampling phase
            if (shouldInterrupt(previousNumInvalidFds, numInvalidFds, numValidFds)) {
                return interrupt();
            }
            previousNumInvalidFds = numInvalidFds;
            benchmark.finish();
        }

        end();

        if (this.executor != null) {
            this.executor.shutdown();
            try {
                this.executor.awaitTermination(365, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    protected abstract void receiveResult(ValidationResult result);

    protected abstract T interrupt();

    protected void end() {

    }

    private boolean shouldInterrupt(int previousNumInvalidFds, int numInvalidFds, int numValidFds) {
        //TODO improve for incremental case
        return (numInvalidFds > numValidFds * this.efficiencyThreshold) && (previousNumInvalidFds < numInvalidFds);
    }

    protected abstract List<OpenBitSet> generateSpecializations(OpenBitSet lhs, int rhs);

    private OpenBitSet flip(OpenBitSet lhs) {
        OpenBitSet flipped = lhs.clone();
        flipped.flip(0, numAttributes);
        return flipped;
    }

    private ValidationResult validate(Collection<LatticeElementLhsPair> currentLevel) throws AlgorithmExecutionException {
        return (this.executor == null) ? this.validateSequential(currentLevel) : this.validateParallel(currentLevel);
    }

    protected abstract void validRhs(LatticeElement elem, int rhs);

    protected abstract void invalidRhs(LatticeElement elem, int rhs);

    static class ValidatorResult {
        private int validations = 0;
        private int pruned = 0;

        int getValidations() {
            return validations;
        }

        int getPruned() {
            return pruned;
        }
    }

    protected static class ValidationResult {
        public final List<IntegerPair> comparisonSuggestions = new ArrayList<>();
        final List<OpenBitSetFD> collectedFDs = new ArrayList<>();
        int validations = 0;
        int intersections = 0;

        public void add(ValidationResult other) {
            this.validations += other.validations;
            this.intersections += other.intersections;
            this.collectedFDs.addAll(other.collectedFDs);
            this.comparisonSuggestions.addAll(other.comparisonSuggestions);
        }
    }

    private class ValidationTask implements Callable<ValidationResult> {
        private LatticeElementLhsPair elementLhsPair;

        ValidationTask(LatticeElementLhsPair elementLhsPair) {
            this.elementLhsPair = elementLhsPair;
        }

        void setElementLhsPair(LatticeElementLhsPair elementLhsPair) {
            this.elementLhsPair = elementLhsPair;
        }

        public ValidationResult call() throws Exception {

            LatticeElement element = this.elementLhsPair.getElement();
            OpenBitSet lhs = this.elementLhsPair.getLhs();
            OpenBitSet rhs = element.getRhsFds();

            ValidationCallback valid = (_lhs, rhsAttr, collectedFDs) -> handleValidRhs(element, lhs, rhsAttr, collectedFDs);
            ValidationCallback invalid = (_lhs, rhsAttr, collectedFDs) -> handleInvalidRhs(element, lhs, rhsAttr, collectedFDs);
            ActualValidator validator = new ActualValidator(plis, compressedRecords, numRecords, valid, invalid, !isTopDown());

            return validator.validate(lhs, rhs);
        }

        private void handleValidRhs(LatticeElement element, OpenBitSet lhs, int rhsAttr, List<OpenBitSetFD> collectedFDs) {
            validRhs(element, rhsAttr);
            if (!isTopDown()) {
                collectedFDs.add(new OpenBitSetFD(lhs.clone(), rhsAttr));
            }
        }

        private void handleInvalidRhs(LatticeElement element, OpenBitSet lhs, int rhsAttr, List<OpenBitSetFD> collectedFDs) {
            invalidRhs(element, rhsAttr);
            if (isTopDown()) {
                collectedFDs.add(new OpenBitSetFD(lhs.clone(), rhsAttr));
            }
        }
    }

}
