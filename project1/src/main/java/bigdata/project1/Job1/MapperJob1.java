package bigdata.project1.Job1;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVParser;



public class MapperJob1 extends Mapper<LongWritable, Text, Text, Text> {
	
	private String regex = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		
		String[] values = new CSVParser().parseLine(line);
		
		if(values.length != 10) 
		{
			System.err.println("invalid line");
			return;
		}
		
		String currentReview = values[8].toLowerCase().replaceAll(regex, " ");
		
		String[] splitReview = currentReview.split(" +");
		Date data = new Date(Long.parseLong(values[7]));
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(data);
		int year = calendar.get(Calendar.YEAR);
		
		for(String current :  splitReview)
			context.write(new Text(year+""), new Text(current));
			
	}
	
	

	
}
