package stubs;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class SentimentPartitionTest {

	SentimentPartitioner spart;

	@Test
	public void testSentimentPartition() {

		spart=new SentimentPartitioner();
		spart.setConf(new Configuration());
		int result;		
		
		/*
		 * A test for word "beauty" with expected outcome 0 would   
		 * look like this:
		 */
		result = spart.getPartition(new Text("beauty"), new IntWritable(23), 3);
		assertEquals(result,0);	

		/*
		 * Test the words "love", "deadly", and "zodiac". 
		 * The expected outcomes should be 0, 1, and 2. 
		 */
        
 		/*
		 * TODO implement
		 */       
		
		// compare if love is partitioned correctly as 0
		result = spart.getPartition(new Text("love"), new IntWritable(23), 3);
		assertEquals(result,0);
		
		// compare if deadly is partitioned correctly as 1
		result = spart.getPartition(new Text("deadly"), new IntWritable(23), 3);
		assertEquals(result,1);
		
		// compare if zodiac is partitioned correctly as 2
		result = spart.getPartition(new Text("zodiac"), new IntWritable(23), 3);
		assertEquals(result,2);
		
	}

}
