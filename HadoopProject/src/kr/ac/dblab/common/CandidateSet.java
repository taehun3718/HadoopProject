package kr.ac.dblab.common;

import java.util.ArrayList;

public class CandidateSet {
	private ArrayList<SeedRecord> seedRecord;
	private int rid;

	public ArrayList<SeedRecord> getSeedRecord() {
		return seedRecord;
	}

	public void setSeedRecord(ArrayList<SeedRecord> seedRecord) {
		this.seedRecord = seedRecord;
	}

	public int getRid() {
		return rid;
	}

	public void setRid(int rid) {
		this.rid = rid;
	}
}