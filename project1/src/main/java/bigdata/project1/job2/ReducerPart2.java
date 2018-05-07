package bigdata.project1.job2;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;

public class ReducerPart2 extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> vals, Reducer<Text, Text, Text, Text>.Context ctx)
			throws IOException, InterruptedException {
		
		
		StringBuilder builder = new StringBuilder();
		
		List<String> sorted = StreamSupport.stream(vals.spliterator(), false).map(Text::toString).collect(Collectors.toList());
		
		Collections.sort(sorted);
		
		sorted.forEach(v -> builder.append(v + " "));
		
		ctx.write(key, new Text(builder.toString()));
		
	}
	
	

}
