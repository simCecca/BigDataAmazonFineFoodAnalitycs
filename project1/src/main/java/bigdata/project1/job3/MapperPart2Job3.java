package bigdata.project1.job3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperPart2Job3 extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable ONE = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		context.write(value, ONE);
		
	}
	
	
	
	
}
