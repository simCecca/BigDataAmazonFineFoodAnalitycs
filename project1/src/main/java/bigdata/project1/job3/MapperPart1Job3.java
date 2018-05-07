package bigdata.project1.job3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVParser;

import bigdata.project1.Job1.Constants;

public class MapperPart1Job3 extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		try {
			String line = value.toString();

			String[] values = new CSVParser().parseLine(line);

			if(values.length != Constants.NUM_OF_FIELDS) {
				System.err.println("invalid Line");
				return;
			}

			String user = values[Constants.USER_ID];
			
			String prodotto = values[Constants.PRODUCT_ID];

			context.write(new Text(user), new Text(prodotto));

		} catch (Exception e) {
			System.err.println("\t\t\t\t\t\t\t\t\t Invalid Line " + e.getMessage());
		}
		
		
	}

}
