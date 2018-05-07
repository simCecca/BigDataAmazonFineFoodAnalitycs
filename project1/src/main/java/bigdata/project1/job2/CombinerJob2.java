package bigdata.project1.job2;

import java.io.IOException;
import java.util.stream.StreamSupport;

import org.apache.hadoop.mapreduce.Reducer;

public class CombinerJob2 extends Reducer<YearProductWritable, Avarage, YearProductWritable, Avarage> {

	@Override
	protected void reduce(YearProductWritable key, Iterable<Avarage> values,
			Reducer<YearProductWritable, Avarage, YearProductWritable, Avarage>.Context context)
			throws IOException, InterruptedException {
		
		Avarage returnValue = new Avarage(0, 0);
		
		double avg = StreamSupport.stream(values.spliterator(), false)
									.mapToDouble((a) -> a.getValueAndIncrement(returnValue).get()).average().getAsDouble();
		returnValue.setValue(avg);
		
		context.write(key, returnValue);
		
	}
	
	

}
