package bigdata.project1.job2;


import java.io.IOException;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerJob2 extends Reducer<YearProductWritable, Avarage, YearProductWritable, DoubleWritable>  {
	
	private int totalWeigth = 0;
	
	private double count(Avarage avg) {
		this.totalWeigth += avg.getCount().get();
		return avg.getValue().get() * avg.getCount().get();
	}
	
	@Override
	protected void reduce(YearProductWritable key, Iterable<Avarage> values,
			Reducer<YearProductWritable, Avarage, YearProductWritable, DoubleWritable>.Context ctx)
			throws IOException, InterruptedException {
		
		this.totalWeigth = 0;
		
		double totAvg = StreamSupport.stream(values.spliterator(), false)
									.mapToDouble(this::count).sum();
		

		
		ctx.write(key, new DoubleWritable(totAvg/totalWeigth));
	}


	

}
