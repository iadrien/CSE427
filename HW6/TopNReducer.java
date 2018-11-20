package stubs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *  Reducer's input are local top N from all mappers.
 *  We have a single reducer, which creates the final top N.
 * 
 * @author Mahmoud Parsian
 *
 */
public class TopNReducer  extends
Reducer<Text, IntWritable, IntWritable, Text> {

	// initialize the N to default value
	private int N = 10; 
	private SortedMap<Integer, Integer> top = new TreeMap<Integer, Integer>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		this.N = context.getConfiguration().getInt("N", 10); 
	}
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		// accumulative movie rating
		int rateSum = 0;
		for (IntWritable value : values) {
			// System.out.println("values: "+value.toString());
			rateSum += value.get();
		}
		
		String movieID = key.toString();
		int id = Integer.valueOf(movieID);
		top.put(rateSum,id);

		// remove extra members from the sorted map
		while(top.size()>N){
			top.remove(top.firstKey());
		}	
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		List<Integer> movieID = new ArrayList<Integer>(top.values());
		List<Integer> ratingSum = new ArrayList<Integer>(top.keySet());
		
//		for(int i=ratingSum.size()-1; i>=0; i--){
//			System.out.println(ratingSum.get(i));
//		}
		
		HashMap<Integer, String> movieLookUp = new HashMap<Integer, String>();
		BufferedReader br = new BufferedReader(new FileReader("movie_titles.txt"));
		
		String idName = "";

		// reading in the movie title txt and keep it in the lookup hash map
		while((idName = br.readLine())!=null){

			String[] info = idName.split(",");
			int ID = Integer.valueOf(info[0]);
			String title = info[2];
			
			// incase some movie names contain the comma 
			for(int index = 3; index < info.length; index++){
				title = title + "," + info[index];
			}
			
			movieLookUp.put(ID,title);
		}

		// output the rating sum and movie name
		for(int index = movieID.size()-1; index>=0; index--){
			int ID = movieID.get(index);
			String title = movieLookUp.get(ID);

			if(title != null){
				context.write(new IntWritable(ratingSum.get(index)), new Text(title));
			}
		}
	}




}