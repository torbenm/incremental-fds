package org.mp.naumann.algorithms.fd.incremental.agreesets;

import java.util.Collection;

class ViolatingPair {

    private final int firstRecord;
    private final int secondRecord;

    ViolatingPair(int a, int b) {
        if (a < b) {
            this.firstRecord = a;
            this.secondRecord = b;
        } else {
            this.firstRecord = b;
            this.secondRecord = a;
        }
    }

    boolean intersects(Collection<Integer> collection) {
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

