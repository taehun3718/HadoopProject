package kr.ac.dblab.common;

import java.util.ArrayList;
import java.util.Collections;

public class NNSamplingData {
	final int k;
	ArrayList<Double> dList;

	public NNSamplingData(int k) {
		dList = new ArrayList<Double>();
		this.k = k;
	}

	public int getSize() {
		return dList.size();
	}

	public double getPrioritySamplingData() {
		return dList.get(k - 1);
	}

	public void poll() {
		dList.remove(k - 1);
		Collections.sort(dList);
	}

	public boolean insertSamplingData(double d) {
		if (this.dList.size() != k) {
			this.dList.add(d);
			Collections.sort(dList);
			return true;
		} else
			return false;

	}

	public boolean compare(double d) {
		return dList.get(k - 1) > d ? true : false;
	}

	public void print() {
		System.out.println("print");
		for (Double d : dList)
			System.out.println(d);
	}

	public double getAverage() {
		double avg = 0;
		for (Double d : dList)
			avg += d;

		return avg / (double) k;
	}

	public double getSum() {
		double sum = 0;
		for (Double d : dList)
			sum += d;

		return sum;
	}

}
