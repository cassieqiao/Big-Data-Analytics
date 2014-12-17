import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class exercise1 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	    private Text year = new Text();
	    private IntWritable temperature = new IntWritable();
	    public static final int MISSING_VALUE = 9999;
    
	    public void configure(JobConf job) {
	    }
    
	    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

	        String line = value.toString();    
	        String yr = line.substring(15,19);
	        int temp;
	        if (line.charAt(87) == '+') {
	    	    temp = Integer.parseInt(line.substring(88,92));
	        } else {
	    	    temp = Integer.parseInt(line.substring(87,92));
	        }
		    String quality = line.substring(92,93);
	        if (temp != MISSING_VALUE && quality.matches("[01459]")) {
	    	    year.set(yr);
		        temperature.set(temp);
	    	    output.collect(year,temperature);
	        }
	    }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	    private IntWritable maxValue = new IntWritable();

        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

            int max = Integer.MIN_VALUE, tmp = 0;
            while(values.hasNext()) {
                tmp = values.next().get();
		        if(tmp > max) {
	                max = tmp;
		        }
	        }
            maxValue.set(max);
	        output.collect(key,maxValue);
        }
    }

    public int run(String[] args) throws Exception {
	    JobConf conf = new JobConf(getConf(), exercise1.class);
	    conf.setJobName("temperature");
    
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(IntWritable.class);
    
	    conf.setMapperClass(Map.class);
	    conf.setCombinerClass(Reduce.class);
	    conf.setReducerClass(Reduce.class);
    
	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);  
    
	    FileInputFormat.setInputPaths(conf, new Path(args[0]));
	    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
	    JobClient.runJob(conf);
	    return 0;
    }

    public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new exercise1(), args);
	    System.exit(res);
    }
}
