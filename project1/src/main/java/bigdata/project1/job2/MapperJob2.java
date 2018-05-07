package bigdata.project1.job2;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVParser;

import bigdata.project1.Job1.Constants;

public class MapperJob2 extends Mapper<LongWritable, Text, YearProductWritable, Avarage> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, YearProductWritable, Avarage>.Context context)
			throws IOException, InterruptedException {
		try {
			String line = value.toString();

			String[] values = new CSVParser().parseLine(line);

			if(values.length != Constants.NUM_OF_FIELDS) {
				System.err.println("invalid Line");
				return;
			}
			
			Calendar calendar = Calendar.getInstance();
			calendar.setTimeInMillis(Long.parseLong(values[Constants.DATE]) * 1000);
			int year = calendar.get(Calendar.YEAR);

			if(year < Constants.MIN_DATE_JOB2) {
				System.err.println("Invalid Date");
				return;
			}
			
			String product = values[Constants.PRODUCT_ID];
			
			int stars = Integer.parseInt(values[Constants.SCORE]);
			
			context.write(new YearProductWritable(year, product), new Avarage(stars));


		} catch (Exception e) {
			System.err.println("Invalid Date / Line" + e.getMessage());
		}
	}

}
