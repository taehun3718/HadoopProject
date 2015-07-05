package kr.ac.dblab.util;

public class Timer {
	String name;
	long start;
	long end;

	public void setTimer(String name) {
		this.name = name;
		start = System.currentTimeMillis();
	}

	public void endTimer() {
		end = System.currentTimeMillis();
	}

	public void printElapsedTime() {
		System.out.println("-------------------------[" + name + "]");
		System.out.println("Total elapsed Time :" + ((end - start) / 1000.0)
				+ "...(" + (end - start) + ")");
		System.out.println("------------------------------------------");
	}

	public double ElapsedTime_double() {
		return ((double) end - (double) start) / 1000.0;
	}
}
