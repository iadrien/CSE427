package stubs;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;

public class IndexMapper extends Mapper<Text, Text, Text, Text> {

  @Override
  public void map(Text key, Text value, Context context) throws IOException,
      InterruptedException {

    /*
     * TODO implement
     */
	  
	  FileSplit fsplit = (FileSplit) context.getInputSplit();
	  Path path = fsplit.getPath();
	  String fileName = path.getName();

	  String location = fileName+"@"+key.toString();
	  
	  String line = value.toString();
	  
	  for (String word : line.split("\\W+")) {
	      if (word.length() > 0) {

	        context.write(new Text(word.toLowerCase()), new Text(location));
	      }
	}
  }
}