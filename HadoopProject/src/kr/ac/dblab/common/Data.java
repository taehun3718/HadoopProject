package kr.ac.dblab.common;

import java.util.StringTokenizer;

public class Data {
	private String R_OR_S = "n/a"; // R일경우 0 else S일 경우 1
	private int rSeedID;
	// private String dataID_coordination;
	private int dataID;
	private double dataX_coordination;
	private double dataY_coordination;

	public Data(String line) {
		StringTokenizer token = new StringTokenizer(line);
		String[] parse_rid_or_sid = token.nextToken().split("#");

		if (parse_rid_or_sid[0].equals("R"))
			R_OR_S = parse_rid_or_sid[0];
		else if (parse_rid_or_sid[0].equals("S"))
			R_OR_S = parse_rid_or_sid[0];

		rSeedID = Integer.parseInt(parse_rid_or_sid[1]);
		String[] parse_r_dataID_or_s_dataID = token.nextToken().split("#");
		// dataID_coordination = parse_r_dataID_or_s_dataID[1] + "\t" +
		// token.nextToken(); //버전 업데이트로 인해 삭제
		dataID = Integer.parseInt(parse_r_dataID_or_s_dataID[1]);
		String[] parse_XYDataCoordination = token.nextToken().split(",");

		dataX_coordination = Double.parseDouble(parse_XYDataCoordination[0]);
		dataY_coordination = Double.parseDouble(parse_XYDataCoordination[1]);
	}

	/**
	 * @return the rSeedID
	 */
	public int getSeedID() {
		return rSeedID;
	}

	/**
	 * @param rSeedID
	 *            the rSeedID to set
	 */
	public void setrSeedID(int rSeedID) {
		this.rSeedID = rSeedID;
	}

	/**
	 * @return the r_OR_S
	 */
	public String getR_OR_S() {
		return R_OR_S;
	}

	/**
	 * @param r_OR_S
	 *            the r_OR_S to set
	 */
	public void setR_OR_S(String r_OR_S) {
		R_OR_S = r_OR_S;
	}

	public int getDataID() {
		return dataID;
	}

	public void setDataID(int dataID) {
		this.dataID = dataID;
	}

	public double getDataX_coordination() {
		return dataX_coordination;
	}

	public void setDataX_coordination(double dataX_coordination) {
		this.dataX_coordination = dataX_coordination;
	}

	public double getDataY_coordination() {
		return dataY_coordination;
	}

	public void setDataY_coordination(double dataY_coordination) {
		this.dataY_coordination = dataY_coordination;
	}
}