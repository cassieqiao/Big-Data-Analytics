import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class exercise2 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

	    private Text year = new Text();
    
	    public void configure(JobConf job) {
	    }
    
	    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	     
            String line = value.toString();
	        String[] tokens = line.split(",");
	        String comb_key = tokens[29].trim() + "," + tokens[30].trim() + "," + tokens[31].trim() + "," + tokens[32].trim();
	        
	        double fourth_col = Double.parseDouble(tokens[3].trim());
	        if (tokens[tokens.length-1].trim().equals("false")) {
	    	    output.collect(new Text(comb_key), new DoubleWritable(fourth_col));
	        }
	    }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            double sum = 0.0, count = 0.0, avg = 0.0;
	        while (values.hasNext()) {
		        sum += values.next().get();
		        count++;
		    }
		    avg = sum/count;
	        output.collect(key, new DoubleWritable(avg));
        }
    }

    public int run(String[] args) throws Exception {
	    JobConf conf = new JobConf(getConf(), exercise2.class);
	    conf.setJobName("ibm");
    
	    conf.setMapOutputKeyClass(Text.class);
	    conf.setMapOutputValueClass(DoubleWritable.class);
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(DoubleWritable.class);
    
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
	    int res = ToolRunner.run(new Configuration(), new exercise2(), args);
	    System.exit(res);
    }
}
