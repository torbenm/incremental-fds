package org.mp.naumann.algorithms.fd.incremental.violations;

public class ViolatingPair {

    private final int firstRecord;
    private final int secondRecord;

    public ViolatingPair(int firstRecord, int secondRecord) {
        this.firstRecord = firstRecord;
        this.secondRecord = secondRecord;
    }

    public int getFirstRecord() {
        return firstRecord;
    }

    public int getSecondRecord() {
        return secondRecord;
    }

    public boolean intersected(int value){
        return firstRecord == value
        || secondRecord == value;
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
