package bigdata.project1.Job1;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CombinerJob1 extends Reducer<IntWritable, WordOccurrences, IntWritable, WordOccurrences> {

	@Override
	protected void reduce(IntWritable key, Iterable<WordOccurrences> vals,
			Reducer<IntWritable, WordOccurrences, IntWritable, WordOccurrences>.Context ctx)
			throws IOException, InterruptedException {
		
		Map<String, Integer> occurrences = StreamSupport.stream(vals.spliterator(), false).collect(Collectors.groupingBy(WordOccurrences::getWordString, Collectors.summingInt(WordOccurrences::getOccurrencesInt)));
		
		occurrences.forEach((word, occ) -> {
			try {
				ctx.write(key, new WordOccurrences(word, occ));
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		});
	}

	
	
}
