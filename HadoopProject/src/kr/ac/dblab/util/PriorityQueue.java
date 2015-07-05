package kr.ac.dblab.util;

import java.util.ArrayList;
import java.util.Collections;

import kr.ac.dblab.common.SeedRecord;

public class PriorityQueue
{
	private final int k;
	private ArrayList<SeedRecord> dList;
	public PriorityQueue(int k)
	{
		setdList(new ArrayList<SeedRecord>());
		this.k = k;
	}
	public void insertData(SeedRecord d)
	{
		if(this.getdList().size()!=getK()*2)
			this.getdList().add(d);		
		else
		{
			//System.out.println("Refused");
		}
		Collections.sort(getdList());
	}
	public boolean isOverlap(int key)
	{
		for(SeedRecord seedr : getdList())
		{
			if(seedr.getSID()==key)
				return true;
		}
		return false;
	}
	public void print()
	{
		System.out.println("print");
		for(SeedRecord d : getdList())
			System.out.println(d.getSID() + "\t" + d.getSx() + "\t" + d.getSy() + "\t" + d.getMinDist());
			
	}
	public String getkNNSID()
	{
		String sID="";
		for(int i=0; i<getK(); i++)
		{
			sID += getdList().get(i).getSID();
			if(i<getdList().size()-1)
				sID +=",";
		}
		//중복 여부를 포함할 case
		if(getdList().size()>getK())
		{
			for(int i = getK(); i<getK()*2; i++)
			{
				if(getdList().get(i)== getdList().get(getK()-1))
					sID += getdList().get(i).getSID() + ",";
				else
					break;
			}
		}
		return sID;
	}
	public double getkNN()//kNN의 k값을 리턴
	{
		//이부분을 바꿔야 함.
		return getdList().get(getK()-1).getMinDist();
	}
	public void pollkNN()
	{
		if(getdList().size()!=0)//kNN 데이터 제거
			getdList().remove(getK()-1);
		else
			//System.out.println("Refused");
			;
	}
	public ArrayList<SeedRecord> getdList() {
		return dList;
	}
	public void setdList(ArrayList<SeedRecord> dList) {
		this.dList = dList;
	}
	public int getK() {
		return k;
	}
}
