package stubs;

import java.util.HashSet;
import java.util.Set;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class SentimentPartitioner extends Partitioner<Text, IntWritable> implements Configurable {

  private Configuration configuration;
  Set<String> positive = new HashSet<String>();
  Set<String> negative = new HashSet<String>();


  @Override
  public void setConf(Configuration configuration) {
    /*
     * TODO implement if necessary
     */
	  
	  String text;
	  
	  // open the positive and negative files
	  File pos = new File("positive-words.txt");
	  File neg = new File("negative-words.txt");
	  
	  InputStreamReader inStream;
	  
	  try{
		  inStream = new InputStreamReader(new FileInputStream(pos));
		  BufferedReader br = new BufferedReader(inStream);
		  
		  // read in the postive words and save to the hash set
		  while((text=br.readLine())!=null){
			  if(text.length()>0 && text.charAt(0)!=';'){
				  positive.add(text);
			  }
		  }
		  
		  inStream = new InputStreamReader(new FileInputStream(neg));
		  br = new BufferedReader(inStream);
		  
		  // read in the negative words and save to the hash set
		  while((text=br.readLine())!=null){
			  if(text.length()>0 && text.charAt(0)!=';'){
				  negative.add(text);
			  }
		  }
		  
		  inStream.close();
		  br.close();
		  
	  }catch(Exception e){
		  e.printStackTrace();
	  }
  }

  /**
   * Implement the getConf method for the Configurable interface.
   */
  @Override
  public Configuration getConf() {
    return configuration;
  }

  /**
   * You need to implement the getPartition method for a partitioner class.
   * This method receives the words as keys (i.e., the output key from the mapper.)
   * It should return an integer representation of the sentiment category
   * (positive, negative, neutral).
   * 
   * For this partitioner to work, the job configuration must have been
   * set so that there are exactly 3 reducers.
   */
  public int getPartition(Text key, IntWritable value, int numReduceTasks) {
    /*
     * TODO implement
     * Change the return 0 statement below to return the number of the sentiment 
     * category; use 0 for positive words, 1 for negative words, and 2 for neutral words. 
     * Use the sets of positive and negative words to find out the sentiment of each word.
     *
     * Hint: use positive.contains(key.toString()) and negative.contains(key.toString())
     * If a word appears in both lists assume it is positive. That is, once you found 
     * that a word is in the positive list you do not need to check if it is in the 
     * negative list. 
     */
     if(positive.contains(key.toString())){
    	 System.out.println(0);
    	 return 0;
     }else if(negative.contains(key.toString())){
    	 System.out.println(1);
    	 return 1;
     }else{
    	 System.out.println(2);
    	 return 2;
     }
  }
}
