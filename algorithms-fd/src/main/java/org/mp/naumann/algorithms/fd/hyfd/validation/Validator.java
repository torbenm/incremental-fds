package org.mp.naumann.algorithms.fd.hyfd.validation;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.AlgorithmExecutionException;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.FDTreeElement;
import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;
import org.mp.naumann.algorithms.fd.structures.plis.PliCollection;
import org.mp.naumann.algorithms.fd.utils.MemoryGuardian;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public abstract class Validator {

    private final static Logger LOG = Logger.getLogger(Validator.class.getName());

    private final FDSet negCover;
    private final FDTree posCover;
    private final PliCollection plis;
    private final float efficiencyThreshold;
    private final MemoryGuardian memoryGuardian;


    private int level = 0;

    Validator(FDSet negCover, FDTree posCover, PliCollection plis, float efficiencyThreshold,
             MemoryGuardian memoryGuardian) {
        this.negCover = negCover;
        this.posCover = posCover;
        this.plis = plis;
        this.efficiencyThreshold = efficiencyThreshold;
        this.memoryGuardian = memoryGuardian;
    }


    protected abstract ValidationResult validate(List<FDTreeElementLhsPair> currentLevel, int level)
            throws AlgorithmExecutionException;


    public List<IntegerPair> validatePositiveCover() throws AlgorithmExecutionException {
        int numAttributes = this.plis.size();

        LOG.info("Validating FDs using plis ...");
        // Start the level-wise validation/discovery
        List<FDTreeElementLhsPair> currentLevel = getInitialLevel(numAttributes);
        int previousNumInvalidFds = 0;
        List<IntegerPair> comparisonSuggestions = new ArrayList<>();
        while (!currentLevel.isEmpty()) {
            LOG.info("\tLevel " + this.level + ": " + currentLevel.size() + " elements; (V)");

            //Validate current level
            ValidationResult validationResult = validate(currentLevel, level);
            comparisonSuggestions.addAll(validationResult.getComparisonSuggestions());

            // If the next level exceeds the predefined maximum lhs size, then we can stop here
            if(isInvalidDepth()){
                printValidationResults(validationResult, 0);
                break;
            }

            // Add all children to the next level
            List<FDTreeElementLhsPair> nextLevel = generateNextLevel(currentLevel, numAttributes);

            // Generate new FDs from the invalid FDs and add them to the next level as well
            int numberOfNewCandidates =
                    generateNewFDsFromInvalidFDsAndAddToNextLevel(validationResult, numAttributes, nextLevel);
            printValidationResults(validationResult, numberOfNewCandidates);

            // Decide if we continue validating the next level or if we go back into the sampling phase
            int numInvalidFds = validationResult.getInvalidFDs().size();
            int numValidFds = validationResult.getValidations() - numInvalidFds;
            if ((numInvalidFds > numValidFds * this.efficiencyThreshold)
                    && (previousNumInvalidFds < numInvalidFds))
                return comparisonSuggestions;

            previousNumInvalidFds = numInvalidFds;
            currentLevel = nextLevel;
            this.level++;
        }

        return null;
    }

    private List<FDTreeElementLhsPair> getInitialLevel(int numAttributes){
        if (this.level == 0) {
            List<FDTreeElementLhsPair> currentLevel = new ArrayList<>();
            currentLevel.add(new FDTreeElementLhsPair(this.posCover, new OpenBitSet(numAttributes)));
            return currentLevel;
        } else {
            return this.posCover.getLevel(this.level);
        }
    }

    private int generateNewFDsFromInvalidFDsAndAddToNextLevel(ValidationResult validationResult,
                                                               int numAttributes, List<FDTreeElementLhsPair> nextLevel){
        LOG.info("(G); ");

        int candidates = 0;
        for (OpenBitFunctionalDependency invalidFD : validationResult.getInvalidFDs()) {
            for (int extensionAttr = 0; extensionAttr < numAttributes; extensionAttr++) {
                OpenBitSet childLhs = this.extendWith(invalidFD.getLhs(), invalidFD.getRhs(), extensionAttr);
                if (childLhs != null) {
                    FDTreeElement child = this.posCover.addFunctionalDependencyGetIfNew(childLhs, invalidFD.getRhs());
                    if (child != null) {
                        nextLevel.add(new FDTreeElementLhsPair(child, childLhs));
                        candidates++;
                        updateMemoryGuardian();
                    }
                }
            }
            if (isInvalidDepth())
                break;
        }
        return candidates;
    }

    private boolean isInvalidDepth(){
        return (this.posCover.getMaxDepth() > -1) && (this.level >= this.posCover.getMaxDepth());
    }

    private List<FDTreeElementLhsPair> generateNextLevel(List<FDTreeElementLhsPair> currentLevel, int numAttributes){
        LOG.info("(C)");

        List<FDTreeElementLhsPair> nextLevel = new ArrayList<>();
        currentLevel
                .stream()
                .map(fdTreeElementLhsPair -> generateNextLevel(fdTreeElementLhsPair, numAttributes))
                .forEach(nextLevel::addAll);
        return nextLevel;
    }

    private List<FDTreeElementLhsPair> generateNextLevel(FDTreeElementLhsPair element, int numAttributes){
        List<FDTreeElementLhsPair> pairs = new ArrayList<>();
        FDTreeElement[] children = element.getElement().getChildren();

        if(children != null) {
            for (int childAttr = 0; childAttr < numAttributes; childAttr++) {
                FDTreeElement child = children[childAttr];

                if (child != null) {
                    OpenBitSet childLhs = element.getLhs().clone();
                    childLhs.set(childAttr);
                    pairs.add(new FDTreeElementLhsPair(child, childLhs));
                }
            }
        }

        return pairs;
    }

    private void printValidationResults(ValidationResult validationResult, int numberOfNewCandidates){
        int numInvalidFds = validationResult.getInvalidFDs().size();
        int numValidFds = validationResult.getValidations() - numInvalidFds;
        LOG.info("(-)(-); "
                + validationResult.getIntersections() + " intersections; "
                + validationResult.getValidations() + " validations; "
                + numInvalidFds + " invalid; "
                + numberOfNewCandidates + " new candidates; --> " + numValidFds + " FDs");

    }

    private void updateMemoryGuardian(){
        this.memoryGuardian.memoryChanged(1);
        this.memoryGuardian.match(this.negCover, this.posCover, null);
    }

    private OpenBitSet extendWith(OpenBitSet lhs, int rhs, int extensionAttr) {
        if (lhs.get(extensionAttr) ||                                            // Triviality: AA->C cannot be valid, because A->C is invalid
                (rhs == extensionAttr) ||                                            // Triviality: AC->C cannot be valid, because A->C is invalid
                this.posCover.containsFdOrGeneralization(lhs, extensionAttr) ||        // Pruning: If A->B, then AB->C cannot be minimal // TODO: this pruning is not used in the Inductor when inverting the negCover; so either it is useless here or it is useful in the Inductor?
                (
                        (this.posCover.getChildren() != null) &&
                                (this.posCover.getChildren()[extensionAttr] != null) &&
                                this.posCover.getChildren()[extensionAttr].isFd(rhs)
                ))    // Pruning: If B->C, then AB->C cannot be minimal

            return null;

        OpenBitSet childLhs = lhs.clone();
        childLhs.set(extensionAttr);

        // Pruning: If A->C, then AB->C cannot be minimal
        if (this.posCover.containsFdOrGeneralization(childLhs, rhs))
            return null;

        return childLhs;
    }

    public PliCollection getPlis() {
        return plis;
    }

}
