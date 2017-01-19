package org.mp.naumann.algorithms.fd.incremental.validator;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.exceptions.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.FDLogger;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.MemoryGuardian;
import org.mp.naumann.algorithms.fd.incremental.datastructures.PositionListIndex;
import org.mp.naumann.algorithms.fd.incremental.pruning.ValidationPruner;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.FDTreeElement;
import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;
import org.mp.naumann.algorithms.fd.utils.FDTreeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public abstract class Validator<T> {

    protected FDSet negCover;
    protected FDTree posCover;
    protected int numRecords;
    protected List<? extends PositionListIndex> plis;
    protected CompressedRecords compressedRecords;
    protected float efficiencyThreshold;
    protected MemoryGuardian memoryGuardian;
    protected ExecutorService executor;
    protected boolean findValid = false;

    protected int level = Integer.MIN_VALUE;
    protected int pruned = 0;
    protected int validations = 0;
    protected final List<ValidationPruner> validationPruners = new ArrayList<>();

    public void addValidationPruner(ValidationPruner ValidationPruner) {
        validationPruners.add(ValidationPruner);
    }

    public Validator(FDSet negCover, FDTree posCover, CompressedRecords compressedRecords, List<? extends PositionListIndex> plis, float efficiencyThreshold, boolean parallel, MemoryGuardian memoryGuardian) {
        this.negCover = negCover;
        this.posCover = posCover;
        this.numRecords = compressedRecords.size();
        this.plis = plis;
        this.compressedRecords = compressedRecords;
        this.efficiencyThreshold = efficiencyThreshold;
        this.memoryGuardian = memoryGuardian;
        if (parallel) {
            int numThreads = Runtime.getRuntime().availableProcessors();
            this.executor = Executors.newFixedThreadPool(numThreads);
        }
    }


    public int getValidations() {
        return validations;
    }

    public int getPruned() {
        return pruned;
    }

    protected class FD {
        public OpenBitSet lhs;
        public int rhs;
        public FD(OpenBitSet lhs, int rhs) {
            this.lhs = lhs;
            this.rhs = rhs;
        }
    }

    protected class ValidationResult {
        public int validations = 0;
        public int intersections = 0;
        public List<FD> invalidFDs = new ArrayList<>();
        public List<FD> validFDs = new ArrayList<>();
        public List<IntegerPair> comparisonSuggestions = new ArrayList<>();
        public void add(ValidationResult other) {
            this.validations += other.validations;
            this.intersections += other.intersections;
            this.invalidFDs.addAll(other.invalidFDs);
            this.validFDs.addAll(other.validFDs);
            this.comparisonSuggestions.addAll(other.comparisonSuggestions);
        }
    }


    protected ValidationResult validateSequential(List<FDTreeElementLhsPair> currentLevel) throws AlgorithmExecutionException {
        ValidationResult validationResult = new ValidationResult();

        Validator.ValidationTask task = new Validator.ValidationTask(null, findValid);
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

    protected ValidationResult validateParallel(List<FDTreeElementLhsPair> currentLevel) throws AlgorithmExecutionException {
        ValidationResult validationResult = new ValidationResult();

        List<Future<ValidationResult>> futures = new ArrayList<>();
        for (FDTreeElementLhsPair elementLhsPair : currentLevel) {
            ValidationTask task = new ValidationTask(elementLhsPair, findValid);
            futures.add(this.executor.submit(task));
        }

        for (Future<ValidationResult> future : futures) {
            try {
                validationResult.add(future.get());
            }
            catch (ExecutionException e) {
                this.executor.shutdownNow();
                e.printStackTrace();
                throw new AlgorithmExecutionException(e.getMessage());
            }
            catch (InterruptedException e) {
                this.executor.shutdownNow();
                e.printStackTrace();
                throw new AlgorithmExecutionException(e.getMessage());
            }
        }

        return validationResult;
    }




    protected List<FDTreeElementLhsPair> pruneLevel(List<FDTreeElementLhsPair> lvl) {
        List<FDTreeElementLhsPair> currentLevel = new ArrayList<>();
        for (FDTreeElementLhsPair fd : lvl) {
            if (validationPruners.stream().anyMatch(ps -> ps.cannotBeViolated(fd))) {
                pruned++;
            } else {
                currentLevel.add(fd);
            }
        }
        FDLogger.log(Level.FINEST, "Will validate: ");
        currentLevel.stream().map(FDTreeElementLhsPair::getLhs).map(BitSetUtils::collectSetBits)
                .forEach(v -> FDLogger.log(Level.FINEST, v.toString()));
        return currentLevel;
    }

    protected OpenBitSet extendWith(OpenBitSet lhs, int rhs, int extensionAttr) {
        if (lhs.get(extensionAttr) || 											// Triviality: AA->C cannot be valid, because A->C is invalid
                (rhs == extensionAttr) || 											// Triviality: AC->C cannot be valid, because A->C is invalid
                this.posCover.containsFdOrGeneralization(lhs, extensionAttr) ||		// Pruning: If A->B, then AB->C cannot be minimal // TODO: this pruning is not used in the IncrementalInductor when inverting the negCover; so either it is useless here or it is useful in the IncrementalInductor?
                ((this.posCover.getChildren() != null) && (this.posCover.getChildren()[extensionAttr] != null) && this.posCover.getChildren()[extensionAttr].isFd(rhs)))
            // Pruning: If B->C, then AB->C cannot be minimal
            return null;

        OpenBitSet childLhs = lhs.clone(); // TODO: This clone() could be avoided when done externally
        childLhs.set(extensionAttr);

        // TODO: Add more pruning here

        // if contains FD: element was a child before and has already been added to the next level
        // if contains Generalization: element cannot be minimal, because generalizations have already been validated
        if (this.posCover.containsFdOrGeneralization(childLhs, rhs))										// Pruning: If A->C, then AB->C cannot be minimal
            return null;

        return childLhs;
    }

    protected class ValidationTask implements Callable<ValidationResult> {
        private FDTreeElementLhsPair elementLhsPair;
        private boolean findValid = false;
        public void setElementLhsPair(FDTreeElementLhsPair elementLhsPair) {
            this.elementLhsPair = elementLhsPair;
        }
        public ValidationTask(FDTreeElementLhsPair elementLhsPair, boolean findValid) {
            this.elementLhsPair = elementLhsPair;
            this.findValid = findValid;
        }
        public ValidationResult call() throws Exception {


            ValidationResult result = new ValidationResult();

            FDTreeElement element = this.elementLhsPair.getElement();
            OpenBitSet lhs = this.elementLhsPair.getLhs();

            OpenBitSet rhs = element.getFds();

            int rhsSize = (int) rhs.cardinality();
            // If we have any issues with insert, check this
            if (rhsSize == 0)
                return result;
            result.validations = result.validations + rhsSize;

            if (Validator.this.level == 0) {
                // Check if rhs is unique
                for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
                    if (!Validator.this.plis.get(rhsAttr).isConstant(Validator.this.numRecords)) {
                        element.removeFd(rhsAttr);
                        if(!findValid){
                            result.invalidFDs.add(new FD(lhs, rhsAttr));
                        }

                    }else if(findValid){
                        result.validFDs.add(new FD(lhs, rhsAttr));
                    }
                    result.intersections++;
                }
            }
            else if (Validator.this.level == 1) {
                // Check if lhs from plis refines rhs
                int lhsAttribute = lhs.nextSetBit(0);
                for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
                    if (!Validator.this.plis.get(lhsAttribute).refines(Validator.this.compressedRecords, rhsAttr)) {
                        element.removeFd(rhsAttr);
                        if(!findValid){
                            result.invalidFDs.add(new FD(lhs, rhsAttr));
                        }
                    }else if(findValid){
                        result.validFDs.add(new FD(lhs, rhsAttr));
                    }
                    result.intersections++;
                }
            }
            else {

                // Check if lhs from plis plus remaining inverted plis refines rhs
                int firstLhsAttr = lhs.nextSetBit(0);
                lhs.clear(firstLhsAttr);
                OpenBitSet validRhs = Validator.this.plis.get(firstLhsAttr).refines(Validator.this.compressedRecords, lhs, rhs, result.comparisonSuggestions);
                lhs.set(firstLhsAttr);

                result.intersections++;

                rhs.andNot(validRhs); // Now contains all invalid FDs
                element.setFds(validRhs); // Sets the valid FDs in the FD tree

                for (int rhsAttr = validRhs.nextSetBit(0); rhsAttr >= 0 && findValid; rhsAttr = validRhs.nextSetBit(rhsAttr + 1)) {
                    result.validFDs.add(new FD(lhs, rhsAttr));
                }

                for (int rhsAttr = rhs.nextSetBit(0); rhsAttr >= 0 && !findValid; rhsAttr = rhs.nextSetBit(rhsAttr + 1))
                    result.invalidFDs.add(new FD(lhs, rhsAttr));
            }
            return result;
        }
    }

    protected void checkMemoryGuardian(){
        this.memoryGuardian.memoryChanged(1);
        this.memoryGuardian.match(this.negCover, this.posCover, null);
    }

    //
    public T validatePositiveCover() throws AlgorithmExecutionException {
        FDLogger.log(Level.FINER, "Validating FDs using plis ...");
        if(level == Integer.MIN_VALUE){
            // First start! set level
            setInitialLevel();
            FDLogger.log(Level.FINER, "Set level to "+level);
        }

        List<FDTreeElementLhsPair> currentLevel = getInitialLevel();

        int previousNumInvalidFds = 0;
        List<IntegerPair> comparisonSuggestions = new ArrayList<>();
        while(checkLevelBounds()){
            FDLogger.log(Level.FINE, "\tLevel " + this.level + ": " + currentLevel.size() + " elements; ");

            // Validate current level
            FDLogger.log(Level.FINER, "(V)");

            validations += currentLevel.size();
            ValidationResult validationResult = (this.executor == null) ? this.validateSequential(currentLevel) : this.validateParallel(currentLevel);
            comparisonSuggestions.addAll(validationResult.comparisonSuggestions);

            if(reachedMaxDepth(validationResult)){
                break;
            }

            FDLogger.log(Level.FINER, "(G); ");
            int candidates = generateNextLevel(validationResult);

            int numInvalidFds, numValidFds;
            if(findValid){
                numValidFds = validationResult.validFDs.size();
                numInvalidFds = validationResult.validations - numValidFds;
            } else {
                numInvalidFds = validationResult.invalidFDs.size();
                numValidFds = validationResult.validations - numInvalidFds;
            }

            FDLogger.log(Level.FINER, validationResult.intersections + " intersections; " + validationResult.validations + " validations; " + numInvalidFds + " invalid; " + candidates + " new candidates; --> " + numValidFds + " FDs");

            // Decide if we continue validating the next level or if we go back into the sampling phase
            if ((numInvalidFds > numValidFds * this.efficiencyThreshold) && (previousNumInvalidFds < numInvalidFds))
                return switchToSampler(comparisonSuggestions);
            currentLevel = FDTreeUtils.getFdLevel(posCover, level);
            previousNumInvalidFds = numInvalidFds;

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

        return terminate(comparisonSuggestions);
    }


    // Abstract methods


    protected abstract boolean checkLevelBounds();
    protected abstract void setInitialLevel();
    protected abstract List<FDTreeElementLhsPair> getInitialLevel();
    protected abstract int generateNextLevel(ValidationResult validationResult);
    protected abstract T switchToSampler(List<IntegerPair> comparisonSuggestions);
    protected abstract T terminate(List<IntegerPair> comparisonSuggestions);
    protected abstract boolean reachedMaxDepth(ValidationResult validationResult);



}


