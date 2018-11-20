package stubs;


import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * TopNDriver: assumes that all K's are unique for all given (K,V) values.
 * Uniqueness of keys can be achieved by using AggregateByKeyDriver job.
 *
 * @author Mahmoud Parsian
 *
 */
public class TopNDriver  extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		
		int exitCode = ToolRunner.run(new Configuration(), new TopNDriver(), args);
		System.exit(exitCode);
	}
	  
	public int run(String[] args) throws Exception{
	    
		// Three arguments: first for the number of top list
		// second and third are the directory for input and output
		if (args.length != 3) {
	      System.out.printf("Usage: <N> <input dir> <output dir>\n");
	      System.exit(-1);
	    }

	    // initialize the variable N
	    int N = Integer.parseInt(args[0]);
	      
	    Configuration conf = getConf();	      
	    conf.setInt("N",N);
	    
	    Job job = new Job(conf);
	    
	    // setup the map reduce configurations
	    job.setJarByClass(TopNDriver.class);
	    job.setJobName("Top N List");


	    FileInputFormat.setInputPaths(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));


	    job.setMapperClass(TopNMapper.class);
	    job.setReducerClass(TopNReducer.class);

	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);

	    boolean success = job.waitForCompletion(true);
	    return success ? 0 : 1;
	  }


}