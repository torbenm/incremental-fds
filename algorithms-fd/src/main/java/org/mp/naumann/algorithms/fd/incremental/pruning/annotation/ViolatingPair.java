package org.mp.naumann.algorithms.fd.incremental.pruning.annotation;

import java.util.Collection;

public class ViolatingPair {

    private final int firstRecord;
    private final int secondRecord;

    public ViolatingPair(int a, int b) {
        if (a < b) {
            this.firstRecord = a;
            this.secondRecord = b;
        } else {
            this.firstRecord = b;
            this.secondRecord = a;
        }
    }

    public int getFirstRecord() {
        return firstRecord;
    }

    public int getSecondRecord() {
        return secondRecord;
    }

    public boolean intersects(int value) {
        return firstRecord == value || secondRecord == value;
    }

    public boolean intersects(Collection<Integer> collection) {
        return collection.contains(firstRecord) || collection.contains(secondRecord);
    }

    @Override
    public String toString() {
        return "(" + firstRecord + "," + secondRecord + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ViolatingPair that = (ViolatingPair) o;

        if (firstRecord != that.firstRecord) return false;
        return secondRecord == that.secondRecord;
    }

    @Override
    public int hashCode() {
        int result = firstRecord;
        result = 31 * result + secondRecord;
        return result;
    }
}

