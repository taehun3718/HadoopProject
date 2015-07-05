package kr.ac.dblab.mapred;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.StringTokenizer;

import kr.ac.dblab.common.Data;
import kr.ac.dblab.common.Ri_data_Si_Adjacent;
import kr.ac.dblab.common.SeedRecord;
import kr.ac.dblab.common.Sj_AdjacenyListForRi;
import kr.ac.dblab.util.PriorityQueue;
import kr.ac.dblab.util.Timer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondMapRed {
	public static class SecondMapRedMapper extends
			Mapper<Object, Text, Text, Text> {

		@Override
		public void setup(Context context) throws IOException {

		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer token = new StringTokenizer(value.toString());

			Text seedRID = new Text(token.nextToken());
			String values = "";

			while (token.hasMoreTokens())
				values += token.nextToken() + "\t";

			context.write(seedRID, new Text(values));
		}
	}

	public static class SecondMapRedReducer extends
			Reducer<Text, Text, Text, Text> {
		int numOfkNN = -1;
		int numOfSeed = -1;
		int numofData = -1; // for r data
		Timer tm;

		public void setup(Context context) throws IOException {
			tm = new Timer();
			tm.setTimer("Reduce end Time");
			Configuration conf = context.getConfiguration();
			numOfkNN = Integer.parseInt(conf.get("kNN"));
			numOfSeed = Integer.parseInt(conf.get("numOfSeed"));
			numofData = Integer.parseInt(conf.get("numOfData"));
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Ri_data_Si_Adjacent record = new Ri_data_Si_Adjacent();

			String[] RiParsing = key.toString().split("#");
			record.setKey(Integer.parseInt(RiParsing[1]));

			for (Text val : values) {

				Data d = new Data(val.toString());
				record.addRiData__siAdjList(d);
				// context.write(key, val);
			}

			boolean overlaped[] = new boolean[numofData * 2];

			Data d;
			Data dforSiAdj;
			Sj_AdjacenyListForRi d2;
			int rID;
			int sID;
			int i, ii;
			int j;
			int k;
			double rx;
			double ry;
			double sx;
			double sy;
			double minDistance; // 프로그램 지역 locality 활용을 위한 상단부 선언
			for (i = 0; i < record.getValue().size(); i++) {

				PriorityQueue pq = new PriorityQueue(numOfkNN);
				for (ii = 0; ii < numofData * 2; ii++)
					overlaped[ii] = false;

				d = record.getValue().get(i);
				// StringTokenizer token = new
				// StringTokenizer(d.getDataID_coordination());

				rID = d.getDataID();

				rx = d.getDataX_coordination();
				ry = d.getDataY_coordination();

				for (j = 0; j < record.getSiAdjList().size(); j++) {
					d2 = record.getSiAdjList().get(j);
					for (k = 0; k < d2.getSeedRecord().size(); k++) {
						dforSiAdj = d2.getSeedRecord().get(k);
						sID = dforSiAdj.getDataID();
						sx = dforSiAdj.getDataX_coordination();
						sy = dforSiAdj.getDataY_coordination();
						minDistance = Math.sqrt(Math.pow(sx - rx, 2)
								+ Math.pow(sy - ry, 2));
						if (pq.getdList().size() != pq.getK()) {
							if (overlaped[sID] != true) {
								pq.insertData(new SeedRecord(sID, sx, sy,
										minDistance));
								overlaped[sID] = true;
							}
						} else if (pq.getkNN() >= minDistance) {
							if (overlaped[sID] != true) {
								pq.pollkNN();
								pq.insertData(new SeedRecord(sID, sx, sy,
										minDistance));
								overlaped[sID] = true;
							}
						}
					}
				}
				context.write(new Text(Integer.toString(rID)),
						new Text(pq.getkNNSID()));
			}
		}

		@Override
		public void cleanup(Context context) throws IOException {
			tm.endTimer();
			String strDT = new SimpleDateFormat("EEE_MMM_dd_HH_mm_ss_SSS",
					Locale.ENGLISH)
					.format(new Date(System.currentTimeMillis()));
			FileSystem fs = FileSystem.get(new Configuration()); // HDFS 파일 제어
																	// 객체
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
					fs.create(new Path("./exp_data/timeStamp/" + strDT + "_"
							+ numofData))));
			bw.write("Elapsed reduce time : " + tm.ElapsedTime_double());
			bw.close();
		}
	}
}