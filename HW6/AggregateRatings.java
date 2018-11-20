package stubs;


import org.apache.hadoop.util.Tool; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

public class AggregateRatings extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new AggregateRatings(),args);
		System.exit(exitCode); 
		 }

  
    
	public int run(String[] args) throws Exception  { 
    
		if (args.length != 2) {     
			System.out.printf("Usage: AggregateByKeyDriver <input dir> <output dir>\n");     
			return -1;
    }

    
    
    Job job = new Job(getConf());    
    job.setJarByClass(AggregateRatings.class);
    
    job.setJobName("Aggregate By Key Driver");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // set classes for mapreduce
    job.setMapperClass(AggregateRatingsMapper.class);
    job.setReducerClass(SumReducer.class);
    job.setCombinerClass(SumReducer.class);
   
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;

  }


}

