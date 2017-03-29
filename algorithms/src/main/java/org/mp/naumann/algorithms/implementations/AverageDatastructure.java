package org.mp.naumann.algorithms.implementations;


/**
 * A datastructure that is capable to store intermediate results for the average algorithm. It does
 * so in the following way:
 *
 * This datastructure stores both the sum and count of all values. Therefore, the {@link
 * AverageIncrementalAlgorithm} only has to take care of updating these variables to the correct
 * values and can derive the new average by diving the sum by the count.
 */
public class AverageDatastructure {

    private double sum = 0;
    private int count = 0;

    public double getSum() {
        return sum;
    }

    public int getCount() {
        return count;
    }

    public void decreaseSum(double deltaSum) {
        this.sum -= deltaSum;
    }

    public void increaseSum(double deltaSum) {
        this.sum += deltaSum;
    }

    public void decreaseCount() {
        decreaseCount(1);
    }

    public void increaseCount() {
        increaseCount(1);
    }

    public void decreaseCount(int deltaCount) {
        if (count - deltaCount < 0) {
            throw new IllegalArgumentException("Count must not be less than 0");
        }
        this.count -= deltaCount;
    }

    public void increaseCount(int deltaCount) {
        if (count + deltaCount < 0) {
            throw new IllegalArgumentException("Count must not be less than 0");
        }
        this.count += deltaCount;
    }

    public Double getAverage() {
        if (count == 0) {
            return null;
        }
        return sum / count;
    }

}