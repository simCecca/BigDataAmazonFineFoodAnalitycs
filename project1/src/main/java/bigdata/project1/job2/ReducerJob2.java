package bigdata.project1.job2;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerJob2 extends Reducer<YearProductWritable, IntWritable, YearProductWritable, DoubleWritable>  {

	@Override
	protected void reduce(YearProductWritable key, Iterable<IntWritable> values,
			Reducer<YearProductWritable, IntWritable, YearProductWritable, DoubleWritable>.Context ctx)
			throws IOException, InterruptedException {
		if (key.getProduct().toString().equals("B009QEBGIQ")) {
			int i = 0;
			for (IntWritable _ : values) i++;
			
			System.out.println("\t\t\t\t\t\t\t\t >>>>>" + i);
		}
		
		double avg = StreamSupport.stream(values.spliterator(), false)
									.mapToInt(IntWritable::get).average().getAsDouble();
		
		ctx.write(key, new DoubleWritable(avg));
	}


	

}
