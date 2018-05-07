package bigdata.project1.job3;

import java.io.IOException;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerPart2Job3 extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> vals,
			Reducer<Text, IntWritable, Text, IntWritable>.Context ctx) throws IOException, InterruptedException {
		
		int sum = StreamSupport.stream(vals.spliterator(), false).mapToInt(IntWritable::get).sum();
		
		
		ctx.write(key, new IntWritable(sum));
		
	}

	
	
}
