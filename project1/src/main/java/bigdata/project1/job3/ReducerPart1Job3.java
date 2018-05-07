package bigdata.project1.job3;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerPart1Job3 extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context ctx)
			throws IOException, InterruptedException {
		
		List<String> vls = StreamSupport.stream(values.spliterator(), false).map(Text::toString).collect(Collectors.toList());
		
		
		
		
	}

}
