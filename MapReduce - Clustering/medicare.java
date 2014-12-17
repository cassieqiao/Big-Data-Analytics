import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.DistributedCache;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.lang.Math;

public class Chu_Fox_Lullo_Qiao_medicare extends Configured implements Tool {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		//array list to store the distributed cache file
		ArrayList <String> cacheFile = new ArrayList <String> ();

		public void configure(JobConf job) {

			try {
				//reading in the cached file and storing it into an array list 
				Path [] localFiles = new Path[0];
				localFiles = DistributedCache.getLocalCacheFiles(job);
				BufferedReader fileIn = new BufferedReader(new FileReader(localFiles[0].toString()));
			
				String fileLine = "";
				while ((fileLine = fileIn.readLine()) != null) {
					cacheFile.add(fileLine);
				}
			}
			catch (FileNotFoundException e1) {
				System.out.println("File not found!");
			}
			catch (IOException e2) {
				System.out.println("IOException!");
			}
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
		    String[] strList = line.split("\t");
		    String closest = "";
		    double minDistance = -1;
		    int vars = cacheFile.get(0).split("\\s+").length - 1;
		    double[] dataPoint = new double[vars];
		    String test = strList[0];

		    if (!test.equals("npi") && !test.equals("0000000001"))
			{
				for(String cluster : cacheFile){
					String[] cacheLine = cluster.split("\\s+");
					double distance = 0;

					//indices of the variables to be clustered 

					//bene_unique_cnt
					//mean = 78.29728, st.dev. = 175.3522
					dataPoint[0] = (Double.parseDouble(strList[19]) - 78.29728)/175.3522;
					//bene_day_srvc_cnt
					//mean = 127.5599, st. dev. = 309.2343
			    	dataPoint[1] = (Double.parseDouble(strList[20]) - 127.5599)/309.2343;
			    	//average_submitted_chg_amt / average_Medicare_allowed_amt
			    	//mean = 3.721821, st. dev. = 28.50376
			    	if(Double.parseDouble(strList[21]) == 0)
			    		dataPoint[2] = 0;
			    	else
			    		dataPoint[2] = (Double.parseDouble(strList[23]) / Double.parseDouble(strList[21]) - 3.721821)/28.50376;
			    	//average_Medicare_payment_amt / average_submitted_chg_amt
			    	//mean = 0.37127, st. dev. = 0.213848
			    	if(Double.parseDouble(strList[23]) == 0)
			    		dataPoint[3] = 0;
			    	else
			    		dataPoint[3] = (Double.parseDouble(strList[25]) / Double.parseDouble(strList[23]) - 0.37127)/0.213848;

					for (int i = 1; i < cacheLine.length; i++) {
						double clusterPoint = Double.valueOf(cacheLine[i]);
						//double dataPoint = Double.valueOf(strList[i - 1]);
						distance = distance + Math.pow(clusterPoint - dataPoint[i - 1], 2);
					}
					
					//cacheLine[0] is the centroid name
					if (minDistance == -1) {
						minDistance = Math.pow(distance, 0.5);
						closest = cacheLine[0];
					}
					else if (Math.pow(distance, 0.5) <= minDistance) {
						minDistance = Math.pow(distance, 0.5);
						closest = cacheLine[0];
					}    
			    }

		    	String valueWrite = "";
		    	for(int i = 0; i < dataPoint.length; i++){
		    		if(valueWrite.equals(""))
		    			valueWrite = String.valueOf(dataPoint[i]);
		    		else
		    			valueWrite = valueWrite + "," + String.valueOf(dataPoint[i]);
		    	}
		    	output.collect(new Text(closest), new Text(valueWrite));
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

			//number of variables
			int vars = 4;
			double[] thisSum = new double[vars];
			int[] thisCount = new int[vars];
			double[] thisCentroid = new double[vars];

			while(values.hasNext()){
				double thisPoint = 0;
				String pointRead = values.next().toString();
				String[] pointList = pointRead.split(",");
				for(int i = 0; i < pointList.length; i++){
					thisPoint = Double.parseDouble(pointList[i]);
					thisSum[i] += Double.parseDouble(pointList[i]);
					thisCount[i] += 1;
				}
			}

			for(int i=0; i < thisSum.length; i++)
				thisCentroid[i] = thisSum[i] / thisCount[i];

			String centroidString = "";
			for(int i = 0; i < thisCentroid.length; i++){
				if(centroidString.equals(""))
			    	centroidString = String.valueOf(thisCentroid[i]);
			    else
			    	centroidString = centroidString + " " + String.valueOf(thisCentroid[i]);
			}

			String newkey = key + ":"+ Integer.toString(thisCount[0]);
			output.collect(new Text(newkey), new Text(centroidString));
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), Chu_Fox_Lullo_Qiao_medicare.class);
		conf.setJobName("clustering");
		//adding the distributed cache file
		DistributedCache.addCacheFile(new Path("clusteringSeeds.txt").toUri(), conf);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
    }

    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Chu_Fox_Lullo_Qiao_medicare(), args);
		System.exit(res);
    }
}