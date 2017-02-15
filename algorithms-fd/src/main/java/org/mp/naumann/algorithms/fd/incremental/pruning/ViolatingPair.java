package org.mp.naumann.algorithms.fd.incremental.pruning;

public class ViolatingPair {

    private final int firstRecord;
    private final int secondRecord;

    public ViolatingPair(int firstRecord, int secondRecord) {
        this.firstRecord = Math.min(firstRecord, secondRecord);
        this.secondRecord = Math.max(firstRecord, secondRecord);
    }

    public int getFirstRecord() {
        return firstRecord;
    }

    public int getSecondRecord() {
        return secondRecord;
    }

    public boolean intersects(int value){
        return firstRecord == value
        || secondRecord == value;
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

