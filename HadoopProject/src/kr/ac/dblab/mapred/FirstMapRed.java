package kr.ac.dblab.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

import kr.ac.dblab.common.AdjSeedS_forSeedRi;
import kr.ac.dblab.common.CandidateSet;
import kr.ac.dblab.common.SeedRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FirstMapRed {
	public static String R_OR_S = "n/a";

	public static class FirstMapRedMapper extends
			Mapper<Object, Text, Text, Text> {
		ArrayList<SeedRecord> seedR_RecordList = new ArrayList<SeedRecord>();
		ArrayList<SeedRecord> seedS_RecordList = new ArrayList<SeedRecord>();
		ArrayList<Double> nnList = new ArrayList<Double>(); // 초기영역 설정을 위한 kNN
															// 평균값 저장
		AdjSeedS_forSeedRi adjSeedS;
		String seedR_Path;
		String seedS_Path;
		String nnList_path;
		int kNN;

		public void loadNNList() throws NumberFormatException, IOException {
			FileSystem fs = FileSystem.get(new Configuration());
			String rLine = "";
			BufferedReader NNList = new BufferedReader(new InputStreamReader(
					fs.open(new Path(nnList_path))));

			if ((rLine = NNList.readLine()) != null) {
				StringTokenizer token = new StringTokenizer(rLine);
				while (token.hasMoreTokens())
					this.nnList.add(Double.parseDouble(token.nextToken()));

			}
			NNList.close();
		}

		public void loadSeedRS() throws IOException {
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader seedR = new BufferedReader(new InputStreamReader(
					fs.open(new Path(seedR_Path))));

			String rLine = "";
			String sLine = "";
			while ((rLine = seedR.readLine()) != null) {
				StringTokenizer token = new StringTokenizer(rLine);

				SeedRecord rRecord = new SeedRecord();
				rRecord.setSID(Integer.parseInt(token.nextToken()));
				rRecord.setSx(Double.parseDouble(token.nextToken()));
				rRecord.setSy(Double.parseDouble(token.nextToken()));

				seedR_RecordList.add(rRecord);
			}
			seedR.close();
			// /Seed R 로드 완료

			BufferedReader seedS = new BufferedReader(new InputStreamReader(
					fs.open(new Path(seedS_Path))));

			while ((sLine = seedS.readLine()) != null) {
				StringTokenizer token = new StringTokenizer(sLine);

				SeedRecord sRecord = new SeedRecord();
				sRecord.setSID(Integer.parseInt(token.nextToken()));
				sRecord.setSx(Double.parseDouble(token.nextToken()));
				sRecord.setSy(Double.parseDouble(token.nextToken()));

				seedS_RecordList.add(sRecord);
			}
			seedS.close();
			// /Seed S 로드 완료
		}

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration(); // Mapper가 가지는
																// context 정보 로드
			// HDFS 파일 시스쳄에 접근하려는 경로 초기화
			seedR_Path = conf.get("seedR_Path");
			seedS_Path = conf.get("seedS_Path");
			nnList_path = conf.get("nnList_path");
			kNN = Integer.parseInt(conf.get("kNN"));
			loadSeedRS();
			loadNNList();

			adjSeedS = new AdjSeedS_forSeedRi();
			// HDFS 파일 시스템에 접근하기 위한 fs객체 생성

			String filename = ((FileSplit) context.getInputSplit()).getPath()
					.getName();
			// /파일 이름이 R_Set이면 R로, 그렇지 않으면 S로
			if (filename.matches(".*R.*"))
				R_OR_S = "R";
			else if (filename.matches(".*S.*"))
				R_OR_S = "S";

			// ///Seed Ri에 대해 인접한 S의 list 후보셋 계산

			// NN 11.393297864780033 14.256845032861746 16.656908638957585
			// 18.72532853575422 20.575070356871123 22.283136236671773
			// 23.873191397794812 25.366754183622927 26.778254434047835
			// 28.131905026248738
			double avg = nnList.get(nnList.size() - 1); // kNN의 초기 영역 설정 값을 불러옴
			// /후보셋 계산을 위한 변수 선언
			double cnt = -1;
			double candPercent = (double) seedR_RecordList.size() * 0.02;
			int pivotCnt = 0;
			// Overlap flag

			for (int i = 0; i < seedR_RecordList.size(); i++) {
				CandidateSet cand = new CandidateSet();
				cnt = 1;// 1<= count <=n
				ArrayList<SeedRecord> CandidateSet = new ArrayList<SeedRecord>();

				int percent = (int) candPercent;
				pivotCnt = 0;
				while (pivotCnt < percent) {
					for (int j = 0; j < seedS_RecordList.size(); j++) {
						double tmpDistance = Math.sqrt(

						Math.pow(seedR_RecordList.get(i).getSx()
								- seedS_RecordList.get(j).getSx(), 2)
								+ Math.pow(seedR_RecordList.get(i).getSy()
										- seedS_RecordList.get(j).getSy(), 2));
						// System.out.println("tmpDist:" + tmpDistance);

						if (tmpDistance <= avg * cnt) {
							boolean isOverapped = false;
							for (int m = 0; m < CandidateSet.size(); m++) {
								if (CandidateSet.get(m).getSID() == seedS_RecordList
										.get(j).getSID()) {
									isOverapped = true;
									break;
								}
							}
							if (isOverapped != true) {
								pivotCnt++;//
								CandidateSet.add(seedS_RecordList.get(j));
							} else {
								// System.out.println("Overapped!");
							}

						}

					}// end of 2nd for
					cnt = cnt + 1; // cnt留뚰겮 諛섏�由�諛섍꼍���볧����먯깋..
				}// end of while
				cand.setSeedRecord(CandidateSet);
				cand.setRid(i);
				adjSeedS.insertCandidateSet(cand);
			}// end of for i
				// 2000Seed 기준 신뢰도97.45%
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// 입력 split으로부터 x,y데이터 파싱
			StringTokenizer itr = new StringTokenizer(value.toString());

			String recordID = itr.nextToken();

			String x1 = itr.nextToken();
			String y1 = itr.nextToken();

			// /1.1 입력 데이터가 R이면 dist거리를 구함
			if (R_OR_S.equals("R")) {
				// 시드R과 가장 가까운 데이터셋R 탐색
				SeedRecord findedNNSeed = dist_forR(Double.parseDouble(x1),
						Double.parseDouble(y1));
				context.write(
						new Text("R#" + Integer.toString(findedNNSeed.getSID())),
						new Text("R#" + Integer.toString(findedNNSeed.getSID())
								+ "\t" + "r#" + recordID + "\t" + x1 + "," + y1));
			}
			if (R_OR_S.equals("S")) {
				// 시드S와 가장 가까운 데이터셋S 탐색
				SeedRecord findedNNSeed = dist_forS(Double.parseDouble(x1),
						Double.parseDouble(y1));

				for (int i = 0; i < adjSeedS.getCandidateSet().size(); i++) {
					int findSID = 0;
					// System.out.println("R" + adjSeedS.candidateSet.get(i).);
					CandidateSet cs = adjSeedS.getCandidateSet().get(i);
					// 찾은 seed와 후보셋으로 선정된 seedID가 일치하면(포함한다면)
					for (SeedRecord sr : cs.getSeedRecord()) {
						if (findedNNSeed.getSID() == sr.getSID()) {
							findSID = findedNNSeed.getSID();
							context.write(
									new Text("R#" + Integer.toString(cs.getRid())),
									new Text("S#" + Integer.toString(findSID)
											+ "\t" + "s#" + recordID + "\t"
											+ x1 + "," + y1));
							break;
						}
					}
					// System.out.println("cand size :" + candCnt);
				}

			}
			// 1.2
		}

		public SeedRecord dist_forR(double x, double y) {
			int recentSID = -1;
			double recentMinDist = Double.MAX_VALUE;
			double tmpCalculatedDist = -1;
			SeedRecord findedNearestNeighbor = new SeedRecord();
			for (SeedRecord sr : seedR_RecordList) {
				tmpCalculatedDist = Math.sqrt(Math.pow(sr.getSx() - x, 2)
						+ Math.pow(sr.getSy() - y, 2));
				if (recentMinDist > tmpCalculatedDist) {
					recentMinDist = tmpCalculatedDist;
					recentSID = sr.getSID();
				} else
					;

			}
			// findedNearestNeighbor.setMinDist(recentMinDist);
			findedNearestNeighbor.setSID(recentSID);
			return findedNearestNeighbor;
		}

		public SeedRecord dist_forS(double x, double y) {
			int recentSID = -1;
			double recentMinDist = Double.MAX_VALUE;
			double tmpCalculatedDist = -1;
			SeedRecord findedNearestNeighbor = new SeedRecord();
			for (SeedRecord sr : seedS_RecordList) // �꾩껜 �쒕뱶 �덉퐫����留뚰겮 諛섎났
			{
				tmpCalculatedDist = Math.sqrt(Math.pow(sr.getSx() - x, 2)
						+ Math.pow(sr.getSy() - y, 2));
				if (recentMinDist > tmpCalculatedDist) {
					recentMinDist = tmpCalculatedDist;
					recentSID = sr.getSID();
				} else
					;

			}
			// findedNearestNeighbor.setMinDist(recentMinDist);
			findedNearestNeighbor.setSID(recentSID);
			return findedNearestNeighbor;
		}
	}

	public static class FirstMapRedReducer extends
			Reducer<Text, Text, Text, Text> {
		@Override
		public void setup(Context context) throws IOException {
			//
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(key, val);
			}

		}
	}
}