import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class StdDev extends Configured implements Tool {

  public static class Map1Std extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    private static DoubleWritable volume = new DoubleWritable();
    private Text s = new Text("s");

    public void configure(JobConf job) {
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
      String line = value.toString();
      String[] entry = line.split("\t");
      volume.set(Double.parseDouble(entry[3]));
      output.collect(s, volume);
    }
  }

  public static class Map2Std extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    private static DoubleWritable volume = new DoubleWritable();
    private Text s = new Text("s");

    public void configure(JobConf job) {
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
      String line = value.toString();
      String[] entry = line.split("\t");
      volume.set(Double.parseDouble(entry[4]));
      output.collect(s, volume);
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
      int n = 0;
      double xI = 0;
      double xBar = 0;
      double xISq = 0;
      double temp = 0;

      while (values.hasNext()) {
        temp = values.next().get();
        n ++;
        xI += temp;
        xISq += temp * temp;
      }

      xBar = xI/n;
      double stdDev = Math.sqrt((xISq - (n * (xBar * xBar))) / n);

      output.collect(key, new DoubleWritable(stdDev));
    }
  }

  public int run(String[] args) throws Exception {
  JobConf conf = new JobConf(getConf(), StdDev.class);
    conf.setJobName("StdDev");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(DoubleWritable.class);

    conf.setMapperClass(Map1Std.class);
    conf.setMapperClass(Map2Std.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, Map1Std.class);
    MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, Map2Std.class);
    FileOutputFormat.setOutputPath(conf, new Path(args[2]));

    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new StdDev(), args);
    System.exit(res);
  }
}