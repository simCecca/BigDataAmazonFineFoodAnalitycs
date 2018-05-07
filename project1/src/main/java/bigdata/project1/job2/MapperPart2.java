package bigdata.project1.job2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperPart2 extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String[] values = value.toString().split("\t");
		
		String product = values[1];
		
		String year = values[0];
		
		String stars = values[2];
		
		context.write(new Text(product), new Text(year + ": " + stars));
		
	}

	
	
}
