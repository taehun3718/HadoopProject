package kr.ac.dblab.common;

import java.util.ArrayList;

// R(i) data r을 담는 클래스
class Ri_data_ri {

	private int key; // Ri : Ri는 SeedR
	private ArrayList<String> value; // data <r1, r2, r3> : r1,r2,r3는 Ri에 대해 거리
										// 기반으로 클러스터링 된 데이터들

	public Ri_data_ri() {
		key = -1;
		value = new ArrayList<String>();
	}

	public void setKV(int key, ArrayList<String> value) {
		if (this.key == -1)
			this.key = key;

		this.value = value;
	}

	public void printData_of_voronoi() {
		for (int i = 0; i < value.size(); i++) {
			// System.out.println(value.get(i));
		}
	}

	public int getKey() {
		return key;
	}

	public ArrayList<String> getValues() {
		return value;
	}
}