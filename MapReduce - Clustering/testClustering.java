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

public class chu_fox_lullo_qiao_testClustering extends Configured implements Tool {

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
	
		public void map(LongWritable key, Text input, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			//reading in the lines from the input file to the mapper
			String line = input.toString();
			String [] parts = line.split("\\s+");
			
			//looping through the centroids; closest is the string to store the cluster name, max is a placeholder variable for the calculated distances
			String closest = "foo";
			Double max = -1.0;
			
			for (String cluster : cacheFile) {
				String [] cacheLine = cluster.split("\\s+");
				Double dist = 0.0;
				
				//the first entry of cacheLine is the cluster name, so the index is offset by 1
				for (int i = 1; i < cacheLine.length; i++) {
					Double clusterPoint = Double.valueOf(cacheLine[i]);
					Double dataPoint = Double.valueOf(parts[i - 1]);
					dist = dist + Math.pow(clusterPoint - dataPoint, 2);
				}
				if (max == -1) {
					max = Math.pow(dist, 0.5);
					closest = cacheLine[0];
				}
				else if (Math.pow(dist, 0.5) <= max) {
					max = Math.pow(dist, 0.5);
					closest = cacheLine[0];
				}
			}

			output.collect(new Text(closest), input);
		}
    	}

//implementing a reducer to recompute new centroid means; no reducer would give us a cluster assignment for every data point
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		    
			//objects to store values needed to calculate the new means
			Double [] sum = new Double[60];
		    	Arrays.fill(sum, 0.0);
			double index = 0.0;
		    
		    	//using the iterator to get the average value for each value of the vector
		   	while (values.hasNext()) {
				String [] parts = values.next().toString().split("\\s+");
				
				//casting all the values from parts into doubles
				Double [] vectorValues = new Double[60];
				for (int i = 0; i < 60; i ++) {
					vectorValues[i] = Double.valueOf(parts[i]);
				}	
				for (int i = 0; i < 60; i++) {
					sum[i] = sum[i] + vectorValues[i];
				}
		    		index += 1;
			}
		    	
			//taking the average and printing it to a space-separated string to emit as output
		    	for (int i = 0; i < 60; i++) {
				sum[i] = sum[i] / index;
			}
			
			String outputString = "";

			for (int i = 0; i < 60; i++) {
				outputString = outputString + Double.toString(sum[i]) + " ";
			}			
			
		    	output.collect(key, new Text(outputString));
		}
	} 

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), chu_fox_lullo_qiao_testClustering.class);
		conf.setJobName("chu_fox_lullo_qiao_testClustering");

		//adding the distributed cache file
		DistributedCache.addCacheFile(new Path("clusteringSeeds.txt").toUri(), conf);
		
		//reducer's output classes		
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
		int res = ToolRunner.run(new Configuration(), new chu_fox_lullo_qiao_testClustering(), args);
		System.exit(res);
    	}
}
