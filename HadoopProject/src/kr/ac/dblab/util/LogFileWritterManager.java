package kr.ac.dblab.util;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;

public class LogFileWritterManager {
	private LogFileWritterManager instance;
	long attempt;
	FileWriter fw;
	BufferedWriter bw;

	public LogFileWritterManager(String logFileName) {
		// http://stackoverflow.com/questions/2885173/java-how-to-create-and-write-to-a-file
		try {
			fw = new FileWriter("/home/dblab/hadoop_v2.2/kth/logs/"
					+ logFileName, true);
			bw = new BufferedWriter(fw);

			attempt = 0;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		instance = this;

	}

	public void printLogAttmpt(String log) {

		try {
			bw.write("[" + Calendar.getInstance().getTime().toString() + "]:"
					+ "iter:" + attempt + "," + log + "\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void printLog(String log) {

		attempt++;
		try {
			bw.write("[" + Calendar.getInstance().getTime().toString() + "]:"
					+ log + "\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void printLogNotWriteDate(String log) {
		attempt++;
		try {
			bw.write("[iter:" + attempt + "]" + log + "\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void closeFile() {
		try {
			bw.flush();
			bw.close();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public LogFileWritterManager getInstance() {
		return instance;
	}
}