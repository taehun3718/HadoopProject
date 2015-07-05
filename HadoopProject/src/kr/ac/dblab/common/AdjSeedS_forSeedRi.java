package kr.ac.dblab.common;

import java.util.ArrayList;

public class AdjSeedS_forSeedRi {
	private ArrayList<CandidateSet> candidateSet;

	public AdjSeedS_forSeedRi() {
		setCandidateSet(new ArrayList<CandidateSet>());
		// TODO Auto-generated constructor stub
	}

	/**
	 * @return the rid
	 */

	public void insertCandidateSet(CandidateSet cand) {
		getCandidateSet().add(cand);
	}

	public ArrayList<CandidateSet> getCandidateSet() {
		return candidateSet;
	}

	public void setCandidateSet(ArrayList<CandidateSet> candidateSet) {
		this.candidateSet = candidateSet;
	}
}