package bigdata.project1.job3;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerPart1Job3 extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context ctx)
			throws IOException, InterruptedException {
		
		Set<String> vls = StreamSupport.stream(values.spliterator(), false).map(Text::toString).collect(Collectors.toSet());
		
		for (String s1 : vls)
			for (String s2 : vls)
				if (s1.compareTo(s2) < 0)
					ctx.write(new Text(s1), new Text(s2));
		
		
	}

}
