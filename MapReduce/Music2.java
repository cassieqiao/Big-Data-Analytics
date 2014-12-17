import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

// This is the name of the outermost class
public class Music2 extends Configured implements Tool {

	// Inner Map class
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

  	// These are the keys, values
  	// artist is the key
    private Text artist = new Text();
    // One is the value
		private static DoubleWritable duration = new DoubleWritable();


		public void configure(JobConf job) {
		}

		// This is the map function
		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    // Takes a line of input and makes it a string
	    String line = value.toString();

	    // Turns the string into discrete tokens
	    String[] splits = line.split(",");
      artist.set(splits[2]);
      duration.set(Double.parseDouble(splits[3]));
      output.collect(artist, duration);
		}
  }

  // Partitioner class
  public static class ArtistPartitioner implements Partitioner<Text, DoubleWritable> {

    @Override
    public int getPartition(Text key, DoubleWritable value, int numReduceTasks) {
      String artist = key.toString().toLowerCase();

      //this is done to avoid performing mod with 0
      if (numReduceTasks == 0) {
        return 0;
      }

      //if the artist is a <= artist <= e, assign partition 1, etc
      if (artist.charAt(0) >= 'a' && artist.charAt(0) <= 'e'){
        return 1 % numReduceTasks;
      } else if (artist.charAt(0) > 'e' && artist.charAt(0) <= 'j') {
        return 2 % numReduceTasks;
      } else if (artist.charAt(0) > 'j' && artist.charAt(0) <= 'o') {
        return 3 % numReduceTasks;
      } else if (artist.charAt(0) > 'o' && artist.charAt(0) <= 't') {
        return 4 % numReduceTasks;
      } else if (artist.charAt(0) > 't' && artist.charAt(0) <= 'z') {
        return 5 % numReduceTasks;
      } else {
        // set other stuff (numbers, random characters) to first partition
        return 1 % numReduceTasks;
      }
    }

    @Override
    public void configure(JobConf conf) {}
  }


  // Inner Reduce class
  public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		// This is the reduce function
		public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	  	// Sets initial max to zero
	    double max = 0;
	    // Loops through list of values
	    while (values.hasNext()) {
        double temp = values.next().get();
        if (temp > max) {
          max = temp;
        }
	    }
	    // Collects each key and the corresponding sum
	    output.collect(key, new DoubleWritable(max));
		}
  }

  // This is a run function that has some configuration stuff
  public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), Music2.class);
		conf.setJobName("Music2");

		// Sets classes of keys, values
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

    // Set partitioner class
    // In $ hadoop jar ... add "-D mapred.reduce.tasks=5"
    conf.setPartitionerClass(ArtistPartitioner.class);

		// Takes input and output paths
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		// Why does it return 0?
		return 0;
  }

  // Main function, calls the necessary things
  public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Music2(), args);
		System.exit(res);
  }
}