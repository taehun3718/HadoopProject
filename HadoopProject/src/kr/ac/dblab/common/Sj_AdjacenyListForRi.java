package kr.ac.dblab.common;

import java.util.ArrayList;

public class Sj_AdjacenyListForRi {
	private ArrayList<Data> seedRecord = new ArrayList<Data>();
	private int seedID;
	
	public ArrayList<Data> getSeedRecord() {
		return seedRecord;
	}
	
	public void setSeedRecord(ArrayList<Data> seedRecord) {
		this.seedRecord = seedRecord;
	}

	public int getSeedID() {
		return seedID;
	}

	public void setSeedID(int seedID) {
		this.seedID = seedID;
	}
}