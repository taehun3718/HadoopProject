package kr.ac.dblab.common;

public class SeedRecord implements Comparable<SeedRecord> {
	private int SID;
	private double sx;
	private double sy;
	private double minDist;

	public SeedRecord() {
	}

	public SeedRecord(int sid, double sx, double sy) {
		this.SID = sid;
		this.sx = sx;
		this.sy = sy;
		minDist = -1;
		// TODO Auto-generated constructor stub
	}

	public SeedRecord(int sid, double sx, double sy, double minDist) {
		this.SID = sid;
		this.sx = sx;
		this.sy = sy;
		this.minDist = minDist;
		// TODO Auto-generated constructor stub
	}

	/**
	 * @return the sID
	 */
	public int getSID() {
		return SID;
	}

	/**
	 * @param sID
	 *            the sID to set
	 */
	public void setSID(int sID) {
		SID = sID;
	}

	/**
	 * @return the sx
	 */
	public double getSx() {
		return sx;
	}

	/**
	 * @param sx
	 *            the sx to set
	 */
	public void setSx(double sx) {
		this.sx = sx;
	}

	/**
	 * @return the sy
	 */
	public double getSy() {
		return sy;
	}

	/**
	 * @param sy
	 *            the sy to set
	 */
	public void setSy(double sy) {
		this.sy = sy;
	}

	/**
	 * @return the minDist
	 */
	public double getMinDist() {
		return minDist;
	}

	/**
	 * @param minDist
	 *            the minDist to set
	 */
	public void setMinDist(double minDist) {
		this.minDist = minDist;
	}

	@Override
	public int compareTo(SeedRecord o) {
		if (minDist > o.minDist)
			return 1;
		else if (minDist < o.minDist)
			return -1;
		else
			return 0;
	}
}
