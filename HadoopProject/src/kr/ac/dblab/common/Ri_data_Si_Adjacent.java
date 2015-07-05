package kr.ac.dblab.common;

import java.util.ArrayList;

//2번째 MapRedcue 파싱을 위한 클래스
//R(i) data	S(i)AdjList 레코드들을 담는 클래스
public class Ri_data_Si_Adjacent {

	private int key; // Ri : Ri는 SeedR
	private ArrayList<Data> value = new ArrayList<Data>(); // Ri에 대한 데이터 리스트
	private ArrayList<Sj_AdjacenyListForRi> siAdjList = new ArrayList<Sj_AdjacenyListForRi>(); // Ri에

	public void setRecordKey(int key) {
		this.key = key;
	}

	public void addRiData__siAdjList(Data d) {
		if (d.getR_OR_S().equals("R"))
			value.add(d);
		else if (d.getR_OR_S().equals("S")) {
			int findSeedIdx = -1;
			Sj_AdjacenyListForRi cand = null;
			for (int i = 0; i < siAdjList.size(); i++) {
				if (siAdjList.get(i).getSeedID() == d.getSeedID()) {
					findSeedIdx = i;
					break;
				}
			}

			if (findSeedIdx == -1) {
				cand = new Sj_AdjacenyListForRi();

				cand.setSeedID(d.getSeedID());
				cand.getSeedRecord().add(d);

				siAdjList.add(cand);
			} else if (findSeedIdx != -1) {
				siAdjList.get(findSeedIdx).getSeedRecord().add(d);
			}
		}
	}

	/**
	 * @return the key
	 */
	public int getKey() {
		return key;
	}

	/**
	 * @param key
	 *            the key to set
	 */
	public void setKey(int key) {
		this.key = key;
	}

	/**
	 * @return the value
	 */
	public ArrayList<Data> getValue() {
		return value;
	}

	/**
	 * @param value
	 *            the value to set
	 */
	public void setValue(ArrayList<Data> value) {
		this.value = value;
	}

	/**
	 * @return the siAdjList
	 */
	public ArrayList<Sj_AdjacenyListForRi> getSiAdjList() {
		return siAdjList;
	}

	/**
	 * @param siAdjList
	 *            the siAdjList to set
	 */
	public void setSiAdjList(ArrayList<Sj_AdjacenyListForRi> siAdjList) {
		this.siAdjList = siAdjList;
	}
}