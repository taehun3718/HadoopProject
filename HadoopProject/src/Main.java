import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.StringTokenizer;

import kr.ac.dblab.common.DataRecord;
import kr.ac.dblab.common.NNSamplingData;
import kr.ac.dblab.common.SamplingData;
import kr.ac.dblab.mapred.FirstMapRed;
import kr.ac.dblab.mapred.SecondMapRed;
import kr.ac.dblab.util.LogFileWritterManager;
import kr.ac.dblab.util.Timer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
	public static void preProcessing(Path input, Path output)
			throws IOException {
		double min = Double.MAX_VALUE;
		double max = 0.;
		double avg = 0.;
		double tmpDist = 0.;
		double sum = 0;
		FileSystem fs = FileSystem.get(new Configuration());

		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(input)));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
				fs.create(output)));

		String line = "";
		ArrayList<DataRecord> dataList = new ArrayList<DataRecord>();
		while ((line = br.readLine()) != null) {
			// System.out.println(line);
			DataRecord d = new DataRecord();
			StringTokenizer iter = new StringTokenizer(line);
			d.setRID(Integer.parseInt(iter.nextToken()));
			d.setX(Double.parseDouble(iter.nextToken()));
			d.setY(Double.parseDouble(iter.nextToken()));
			dataList.add(d);
		}// �꾩껜 �곗씠��遺덈윭��

		long cnt = 0;
		// System.out.println("sz + " + dataList.size());
		for (DataRecord d1_1 : dataList) {
			for (DataRecord d1_2 : dataList) {
				tmpDist = Math.sqrt(Math.pow(d1_1.getX() - d1_2.getX(), 2)
						+ Math.pow(d1_1.getY() - d1_2.getY(), 2));

				if (tmpDist >= max)
					max = tmpDist;
				if (tmpDist <= min && tmpDist != 0)
					min = tmpDist;
				sum += tmpDist;
				cnt++;
			}
		}
		avg = sum / cnt;
		// System.out.println("Max: " + max);
		// System.out.println("Min: " + min);
		// System.out.println("Avg: " + avg);
		br.close();
		bw.write(max + "\t" + min + "\t" + avg + "\n");
		bw.close();
	}

	public static void preProcessing(Path input, Path output, Path output_kNNAvg)
			throws IOException {
		double min = Double.MAX_VALUE;
		double max = 0.;
		double avg = 0.;
		double tmpDist = 0.;
		double sum = 0;
		double kNNsum = 0;

		FileSystem fs = FileSystem.get(new Configuration());

		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(input)));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
				fs.create(output)));

		BufferedWriter bw_nn = new BufferedWriter(new OutputStreamWriter(
				fs.create(output_kNNAvg)));

		String line = "";
		ArrayList<SamplingData> SamplingDataList = new ArrayList<SamplingData>();
		while ((line = br.readLine()) != null) {
			// System.out.println(line);
			SamplingData d = new SamplingData();
			StringTokenizer iter = new StringTokenizer(line);
			d.setRID(Integer.parseInt(iter.nextToken()));
			d.setX(Double.parseDouble(iter.nextToken()));
			d.setY(Double.parseDouble(iter.nextToken()));
			SamplingDataList.add(d);
		}
		br.close();
		long cnt = 0;
		// System.out.println("sz + " + SamplingDataList.size());
		// System.out.println("Preprocessing...");
		// timer.setTimer("Preprocessing");
		// System.out.println(0 + "/" + 100 );
		int i = 0;
		String line2 = "";
		for (int k = 10; k <= 10; k++) {
			min = Double.MAX_VALUE;
			i = 0;
			cnt = 0;
			max = 0.;
			avg = 0.;
			tmpDist = 0.;
			sum = 0;
			kNNsum = 0;
			for (SamplingData d1_1 : SamplingDataList) {
				NNSamplingData mSamplingData = new NNSamplingData(k);
				for (SamplingData d1_2 : SamplingDataList) {
					tmpDist = Math.sqrt(Math.pow(d1_1.getX() - d1_2.getX(), 2)
							+ Math.pow(d1_1.getY() - d1_2.getY(), 2));
					if (tmpDist >= max)
						max = tmpDist;
					if (mSamplingData.getSize() != k) {
						if (tmpDist != 0.0) {
							mSamplingData.insertSamplingData(tmpDist);
							min = tmpDist;
						}
					} else if (tmpDist <= mSamplingData
							.getPrioritySamplingData() && tmpDist != 0) {
						min = tmpDist;
						mSamplingData.poll();
						mSamplingData.insertSamplingData(tmpDist);
					}
					sum += tmpDist;
					cnt++;
				}
				kNNsum += mSamplingData.getAverage();
				i++;
				// System.out.println(kNNsum);
			}
			// System.out.println(kNNsum);
			// System.out.println("I:" + i);
			line2 += (kNNsum) / i + "\t";
			// System.out.println(i + "\t" + i*k + "\t");
			// System.out.println(k + "NN Average\t" + (kNNsum)/((double)i*k) );
			// avg = sum/cnt;
			// System.out.println("Max: " + max);
			// System.out.println("Min: " + min);
			// System.out.println("Avg: " + avg);

		}
		avg = sum / cnt;
		bw_nn.write(line2);
		bw_nn.close();

		// System.out.println("Max: " + max);
		// System.out.println("Min: " + min);
		// System.out.println("Avg: " + avg);
		// System.out.println("nn " + line2);

		bw.write(max + "\t" + min + "\t" + avg + "\n");
		bw.close();
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		String dataK[] = { "250K" };// , "150K", "200K", "250K"};
									// //100k-->100만개로 정의해주세요. (원래는 1000K)
		// String dataK[] = {"100K"};
		// String totalData = "1000000";
		String totalData = "2500000";
		int numOfreduce = 10;
		String kNN[] = { "100" };
		// String kNN[] = {"100"};
		// String samplingN[] = {"2000", "4000", "6000", "8000"};
		String samplingN[] = { "2000" };
		// String kNN[] = {"100"};
		// String samplingN[] = {"500", "1000", "1500"};
		for (String dataSet : dataK) {
			for (String NumofSampling : samplingN) {
				System.out.print("Auto mode" + NumofSampling);
				for (String NumOfkNN : kNN) {
					System.out.println("_" + NumOfkNN);
					String samplingPathR = "./exp_data/Samp_R_" + NumofSampling
							+ "_" + dataSet;
					String samplingPathS = "./exp_data/Samp_S_" + NumofSampling
							+ "_" + dataSet;

					String preprocessingPath = "./OurScheme/preprocessing/"
							+ "prep_R_" + NumofSampling + ".txt";
					String firstMapRedOutputPath = "./OurScheme/1stMapRed_"
							+ NumOfkNN + "NN_" + NumofSampling + "Samp"
							+ dataSet;
					String secondMapRedOutputPath = "./OurScheme/2ndMapRed_"
							+ NumOfkNN + "NN_" + NumofSampling + "Samp"
							+ dataSet;
					int i = 0;
					// if(NumofSampling.equals("8000")
					// && ( NumOfkNN.equals("20") ))
					// i=5;
					// if(NumofSampling.equals("8000") && (
					// NumOfkNN.equals("40")))
					for (; i < 5; i++) {
						// String strDT =
						// new
						// SimpleDateFormat("[EEE MMM dd HH:mm:ss:SSS z yyyy]::",
						// Locale.ENGLISH).format(
						// new Date(System.currentTimeMillis()));
						LogFileWritterManager lwm = new LogFileWritterManager(
								"_" + dataSet + "_" + NumOfkNN + "NN_reduce"
										+ numOfreduce + "_" + NumofSampling
										+ ".txt");

						double total_elapsedTimer = 0.0;
						Timer timer = new Timer();

						Configuration conf = new Configuration();

						System.out.println("Delete FirstMapRedPath");
						Runtime.getRuntime().exec(
								"bin/hdfs dfs -rmr " + firstMapRedOutputPath);
						Thread.sleep(5000);
						System.out.println("Delete SecondMapRedPath");
						Runtime.getRuntime().exec(
								"bin/hdfs dfs -rmr " + secondMapRedOutputPath);
						Thread.sleep(5000);
						System.out.println();
						System.out
								.println("Our scheme 2.0.609.18_finished             : 140610 02:36");
						System.out
								.println("Our scheme 2.0.609.19_overlap optimization");
						System.out
								.println("Our scheme 2.0.809.01_candidate_changed");
						System.out
								.println("Our scheme 2.0.714.00_auto experimentation seting");
						System.out.println("Our scheme 2.0.810.00_Cand 2%");
						System.out.println("Our scheme 2.0.810.17_..."
								+ dataSet);
						System.out
								.println("-------------------------------------------------------");
						// Preprocessing
						System.out.println("Preprocessing");

						timer.setTimer("Preprocessing");

						preProcessing(new Path("./" + samplingPathR + "/"
								+ "part-r-00000"), new Path("./"
								+ preprocessingPath), new Path(
								"./OurScheme/preprocessing/" + "NNList"
										+ "0_to_" + NumOfkNN + ".txt"));
						// preProcessing(
						// new Path("./" + setup.getSamplingPathS() + "/" +
						// "part-r-00000"),
						// new Path("./" + setup.getPreprocessingPath() + "/" +
						// setup.getPrep_S()) );

						// OK.
						// //84.44912531212262 100.37257795770127
						// 113.8650626565014 129.14058116406324
						// 140.48634044211693 157.2058308367725
						// 173.08324741900407 189.9145160465739
						// 212.32579961873074 191.09321965685768

						// HDFS
						// R : Max Min Avg, S: Max Min Avg

						timer.endTimer();

						total_elapsedTimer += timer.ElapsedTime_double();
						lwm.printLog("Preprocessing Time : "
								+ Double.toString(timer.ElapsedTime_double()));
						timer.printElapsedTime();

						conf.set("seedR_Path", "./" + samplingPathR + "/"
								+ "part-r-00000");
						conf.set("seedS_Path", "./" + samplingPathS + "/"
								+ "part-r-00000");
						conf.set("kNN", NumOfkNN);
						conf.set("nnList_path", "./OurScheme/preprocessing/"
								+ "NNList" + "0_to_" + NumOfkNN + ".txt");

						Job job = new Job(conf, "Our-Scheme_firstMapRed"
								+ NumOfkNN + "_" + NumofSampling); // Job
						job.setJarByClass(FirstMapRed.class);
						job.setMapperClass(FirstMapRed.FirstMapRedMapper.class);
						job.setReducerClass(FirstMapRed.FirstMapRedReducer.class);
						// Set Mapper, Reducer
						job.setNumReduceTasks(numOfreduce); // Reducer
						// Format

						job.setOutputKeyClass(Text.class); // Text
						job.setOutputValueClass(Text.class);

						FileInputFormat.addInputPath(job, new Path("./"
								+ dataSet + "_R_Set/" + "R_" + dataSet
								+ "_2d.txt")); // R_Object
						FileInputFormat.addInputPath(job, new Path("./"
								+ dataSet + "_S_Set/" + "S_" + dataSet
								+ "_2d.txt")); // S_Object //split
						// FileInputFormat.addInputPath(job, new Path("." +
						// "/Object_R_S_2d/" + "R_564932_2d.txt") ); //R_Object
						// FileInputFormat.addInputPath(job, new Path("." +
						// "/Object_R_S_2d/" + "S_564932_2d.txt") ); //S_Object
						// //split

						FileOutputFormat.setOutputPath(job, new Path(
								firstMapRedOutputPath)); // 1stMapReduce result
						timer.setTimer("1stMapReduce");
						job.waitForCompletion(true);
						timer.endTimer();
						total_elapsedTimer += timer.ElapsedTime_double();
						lwm.printLog("1stMapReduce Time : "
								+ Double.toString(timer.ElapsedTime_double()));
						timer.printElapsedTime();
						// System.exit(0);
						conf.set("kNN", NumOfkNN);
						conf.set("numOfSeed", NumofSampling);
						conf.set("numOfData", totalData);
						job = new Job(conf, "Our-Scheme_secondMapRed"
								+ NumOfkNN + "_" + NumofSampling); // Job
						job.setJarByClass(SecondMapRed.class);
						job.setMapperClass(SecondMapRed.SecondMapRedMapper.class);
						job.setReducerClass(SecondMapRed.SecondMapRedReducer.class);
						job.setNumReduceTasks(numOfreduce);
						job.setOutputKeyClass(Text.class); // Text
						job.setOutputValueClass(Text.class);

						FileInputFormat.addInputPath(job, new Path(
								firstMapRedOutputPath + "/" + "part-r-00000")); // 1stMapRed
																				// result
						FileInputFormat.addInputPath(job, new Path(
								firstMapRedOutputPath + "/" + "part-r-00001")); // 1stMapRed
																				// result
						FileInputFormat.addInputPath(job, new Path(
								firstMapRedOutputPath + "/" + "part-r-00002")); // 1stMapRed
																				// result
						FileInputFormat.addInputPath(job, new Path(
								firstMapRedOutputPath + "/" + "part-r-00003")); // 1stMapRed
																				// result
						FileInputFormat.addInputPath(job, new Path(
								firstMapRedOutputPath + "/" + "part-r-00004")); // 1stMapRed
																				// result
						FileInputFormat.addInputPath(job, new Path(
								firstMapRedOutputPath + "/" + "part-r-00005")); // 1stMapRed
																				// result
						FileInputFormat.addInputPath(job, new Path(
								firstMapRedOutputPath + "/" + "part-r-00006")); // 1stMapRed
																				// result
						FileInputFormat.addInputPath(job, new Path(
								firstMapRedOutputPath + "/" + "part-r-00007")); // 1stMapRed
																				// result
						FileInputFormat.addInputPath(job, new Path(
								firstMapRedOutputPath + "/" + "part-r-00008")); // 1stMapRed
																				// result
						FileInputFormat.addInputPath(job, new Path(
								firstMapRedOutputPath + "/" + "part-r-00009")); // 1stMapRed
																				// result
						FileOutputFormat.setOutputPath(job, new Path(
								secondMapRedOutputPath)); // 1stMapReduce result

						timer.setTimer("2ndMapReduce");
						job.waitForCompletion(true);
						timer.endTimer();
						total_elapsedTimer += timer.ElapsedTime_double();
						lwm.printLog("2ndMapReduce Time : "
								+ Double.toString(timer.ElapsedTime_double()));
						timer.printElapsedTime();
						System.out.println("Total elapsedTime : "
								+ total_elapsedTimer);
						lwm.printLog("Total elapsedTime : "
								+ Double.toString(total_elapsedTimer));
						lwm.closeFile();
						Thread.sleep(10000);
						// job.setReducerClass(SecondMapRed.SecondMapRedReducer.class);
						// System.exit(0);
						// ////////////////////////////////////////////////////////////////////////////////////////////////////////////
					}
				}// end of second For
			}// end of first For
		}
	}
}
