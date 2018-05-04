package bigdata.project1.Job1;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJob1 extends Reducer<IntWritable, Text, IntWritable, Text> {

	public Map<String, Long> getSortedOccurrences(Iterable<Text> vals) {

		Map<String, Long> occurrences = StreamSupport.stream(vals.spliterator(), true).collect(Collectors.groupingBy(Text::toString, Collectors.counting()));
		
		NavigableMap<String, Long> sortedOccurrences = new TreeMap<>((a, b) -> {
			int res = (int)(occurrences.get(b) - occurrences.get(a));
			if (res == 0) res = a.compareTo(b);
			return res;
		});

		occurrences.entrySet().forEach(e -> {
			sortedOccurrences.put(e.getKey(), e.getValue());

			if (sortedOccurrences.size() > 10)
				sortedOccurrences.pollLastEntry();

		});

		return sortedOccurrences;

	}


	@Override
	protected void reduce(IntWritable key, Iterable<Text> vals,
			Reducer<IntWritable, Text, IntWritable, Text>.Context ctx) throws IOException, InterruptedException {

		Map<String, Long> sortedOccurrences = getSortedOccurrences(vals);

		StringBuilder builder = new StringBuilder();

		sortedOccurrences.entrySet().forEach(e -> builder.append(e.getKey() + " " +e.getValue() + " "));

		ctx.write(key, new Text(builder.toString()));

	}



}
