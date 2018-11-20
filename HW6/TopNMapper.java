package stubs;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *  Mapper's input are read from SequenceFile and records are: (K, V)
 *    where 
 *          K is a Text
 *          V is an Integer
 * 
 * @author Mahmoud Parsian
 *
 */
public class TopNMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	// set the default N if none is provided (however the program require three arguments)
	int N = 10;
	private SortedMap<Integer, Integer> top;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {		
		top = new TreeMap<Integer, Integer>();
		N = context.getConfiguration().getInt("N", 10); 	   
	}
	
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// parse the input data and make the corresponding mapper output
		String text = value.toString();	   
		String[] lines = text.split("/r/n");
	   
		for (String line:lines){		   
			String[] info = line.split(",");		   
			String movieID = info[0];		   
			String movieRate = info[2];		   
			
			// movie ratings are cast to integers because after observing the data
			// it seems that there is only integers though they are written in double format
			int rate = (int) Double.parseDouble(movieRate);		   
			context.write(new Text(movieID), new IntWritable(rate));
	   }
      
   }
   

  

}