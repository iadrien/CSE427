package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends
    Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    /*
     * TODO implement
     */
    String line = value.toString();
    line = line.toLowerCase();
    
    String[] wordList = line.split("\\W+");
    
 	    String word1 = wordList[0];
 	    String word2 = wordList[1];

 	    
 	    String keyK = word1 + "," + word2;
        context.write(new Text(keyK), new IntWritable(1));
 
  }
}
