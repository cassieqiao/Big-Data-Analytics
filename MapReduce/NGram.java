import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class NGram extends Configured implements Tool {

  public static class Map1Gram extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

		private static DoubleWritable volume = new DoubleWritable();
		private Text wordyear = new Text();

		public void configure(JobConf job) {
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
		  String line = value.toString();
			String[] entry = line.split("\t");
			String input = entry[0];
			String word = input.toLowerCase();
			int year = Integer.parseInt(entry[1]);
			double vol = Double.parseDouble(entry[3]);
			// String word1;
			// String word2;
			// if (word.contains(" ")) {
			// 	String[] splited = word.split("\\s+");
			// 	word1 = splited[0];
			// 	word2 = splited[1];
			// } else {
			// 	word1 = word;
			// 	word2 = "";
			// }
			if (word.contains("nu")) {
				wordyear.set(Integer.toString(year) + " " + "nu");
				volume.set(vol);
				output.collect(wordyear, volume);
			}
			if (word.contains("die")) {
				wordyear.set(Integer.toString(year) + " " + "die");
				volume.set(vol);
				output.collect(wordyear, volume);
			}
			if (word.contains("kla")) {
				wordyear.set(Integer.toString(year) + " " + "kla");
				volume.set(vol);
				output.collect(wordyear, volume);

			}
			// if (word2.contains("nu"))
			// {
			// 	wordyear.set(Integer.toString(year)+" "+"nu");
			// 	volume.set(vol);
			// 	output.collect(wordyear, volume);
			// }
			// if (word2.contains("die"))
			// {
			// 	wordyear.set(Integer.toString(year)+" "+"die");
			// 	volume.set(vol);
			// 	output.collect(wordyear, volume);
			// }
			// if (word2.contains("kla"))
			// {
			// 	wordyear.set(Integer.toString(year)+" "+"kla");
			// 	volume.set(vol);
			// 	output.collect(wordyear, volume);
			// }

		}
	}

	public static class Map2Gram extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

		private static DoubleWritable volume = new DoubleWritable();
		private Text wordyear = new Text();

		public void configure(JobConf job) {
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
		  String line = value.toString();
			String[] entry = line.split("\t");
			String input1 = entry[0];
			String word1 = input1.toLowerCase();
			String input2 = entry[1];
			String word2 = input2.toLowerCase();
			String word = word1 + " " + word2;
			int year = Integer.parseInt(entry[2]);
			double vol = Double.parseDouble(entry[4]);
			// String word1;
			// String word2;
			// if (word.contains(" ")) {
			// 	String[] splited = word.split("\\s+");
			// 	word1 = splited[0];
			// 	word2 = splited[1];
			// } else {
			// 	word1 = word;
			// 	word2 = "";
			// }
			if (word.contains("nu")) {
				wordyear.set(Integer.toString(year) + " " + "nu");
				volume.set(vol);
				output.collect(wordyear, volume);
			}
			if (word.contains("die")) {
				wordyear.set(Integer.toString(year) + " " + "die");
				volume.set(vol);
				output.collect(wordyear, volume);
			}
			if (word.contains("kla")) {
				wordyear.set(Integer.toString(year) + " " + "kla");
				volume.set(vol);
				output.collect(wordyear, volume);

			}
			// if (word2.contains("nu"))
			// {
			// 	wordyear.set(Integer.toString(year)+" "+"nu");
			// 	volume.set(vol);
			// 	output.collect(wordyear, volume);
			// }
			// if (word2.contains("die"))
			// {
			// 	wordyear.set(Integer.toString(year)+" "+"die");
			// 	volume.set(vol);
			// 	output.collect(wordyear, volume);
			// }
			// if (word2.contains("kla"))
			// {
			// 	wordyear.set(Integer.toString(year)+" "+"kla");
			// 	volume.set(vol);
			// 	output.collect(wordyear, volume);
			// }

		}
	}

  public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			double sum = 0.0;
			int count = 0;
		  while (values.hasNext()) {
				sum += values.next().get();
				count ++;
		  }
		  double avg = sum/count;
		  output.collect(key, new DoubleWritable(avg));
		}
	}

  public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), NGram.class);
		conf.setJobName("NGram");

		conf.setOutputKeyClass(Text.class);
	  conf.setOutputValueClass(DoubleWritable.class);

		conf.setMapperClass(Map1Gram.class);
		conf.setMapperClass(Map2Gram.class);
		// conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// FileInputFormat.setInputPaths(conf, new Path(args[0]));
		// FileInputFormat.setInputPaths(conf, new Path(args[1]));
		MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, Map1Gram.class);
		MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, Map2Gram.class);
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		JobClient.runJob(conf);
		return 0;
  }

  public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new NGram(), args);
		System.exit(res);
  }
}