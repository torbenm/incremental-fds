package org.mp.naumann.algorithms.fd.hyfd.validation;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.IntegerPair;

import java.util.ArrayList;
import java.util.List;

class ValidationResult {

    private int validations = 0;
    private int intersections = 0;
    private final List<OpenBitFunctionalDependency> invalidFDs = new ArrayList<>();
    private final List<IntegerPair> comparisonSuggestions = new ArrayList<>();

    void add(ValidationResult other) {

        this.validations += other.validations;
        this.intersections += other.intersections;
        this.invalidFDs.addAll(other.invalidFDs);
        this.comparisonSuggestions.addAll(other.comparisonSuggestions);
    }

    void addInvalidFd(OpenBitFunctionalDependency fd){
        invalidFDs.add(fd);
    }
    void addInvalidFd(OpenBitSet lhs, int rhsAttr){
        invalidFDs.add(new OpenBitFunctionalDependency(lhs, rhsAttr));
    }

    void incrementIntersections(){
        intersections++;
    }

    void incrementValidations(int amount){
        this.validations += amount;
    }

    public List<IntegerPair> getComparisonSuggestions() {
        return comparisonSuggestions;
    }

    public int getValidations() {
        return validations;
    }

    public int getIntersections() {
        return intersections;
    }

    public List<OpenBitFunctionalDependency> getInvalidFDs() {
        return invalidFDs;
    }
}