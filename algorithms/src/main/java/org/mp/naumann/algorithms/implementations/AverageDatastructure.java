package org.mp.naumann.algorithms.implementations;

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
	
	public double getAverage() {
		return sum / count;
	}

}