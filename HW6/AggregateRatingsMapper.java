package stubs;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AggregateRatingsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString().trim();
    String[] tokens =line.split(",");

    if (tokens.length != 3) {
        return;
    }
    
    String id = tokens[0];
    int rating = (int)Double.parseDouble(tokens[2]);
    context.write(new Text(id), new DoubleWritable(rating));
  }
}