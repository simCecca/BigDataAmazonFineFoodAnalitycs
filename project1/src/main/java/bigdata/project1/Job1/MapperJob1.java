package bigdata.project1.Job1;

import java.io.IOException;


import java.util.Calendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVParser;



public class MapperJob1 extends Mapper<LongWritable, Text, IntWritable, WordOccurrences> {

	private static final String SEPARATORS = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, WordOccurrences>.Context context)
			throws IOException, InterruptedException {

		try {
			String line = value.toString();

			String[] values = new CSVParser().parseLine(line);

			if(values.length != Constants.NUM_OF_FIELDS) {
				System.err.println("invalid Line");
				return;
			}

			String currentReview = values[Constants.SUMMARY].toLowerCase().replaceAll(SEPARATORS, " ");

			String[] splitReview = currentReview.split(" +");

			Calendar calendar = Calendar.getInstance();
			calendar.setTimeInMillis(Long.parseLong(values[Constants.DATE]) * 1000);
			int year = calendar.get(Calendar.YEAR);

			if(year < Constants.MIN_DATE_JOB1) {
				System.err.println("Invalid Date");
				return;
			}

			for(String current : splitReview)
				context.write(new IntWritable(year), new WordOccurrences(current, 1));

		} catch (Exception e) {
			System.err.println("Invalid Date / Line" + e.getMessage());
		}
	}




}
