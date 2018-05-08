package bigdata.spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Calendar;


import com.opencsv.CSVParser;


public class RecordParser implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	
	private static final String SEPARATORS = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";
	
	public Record parseRecord(String line) {
		Record record = new Record();
		
		try {
			String[] values = new CSVParser().parseLine(line);

			if(values.length != Constants.NUM_OF_FIELDS) {
				System.err.println("invalid Line");
				return null;
			}

			String currentReview = values[Constants.SUMMARY].toLowerCase().replaceAll(SEPARATORS, " ");

			String[] reviewWords = currentReview.split(" +");

			Calendar calendar = Calendar.getInstance();
			calendar.setTimeInMillis(Long.parseLong(values[Constants.DATE]) * 1000);
			int year = calendar.get(Calendar.YEAR);

			if(year < Constants.MIN_DATE_JOB1) {
				System.err.println("Invalid Date");
				return null;
			}

			record.setProduct(values[Constants.PRODUCT_ID]);
			record.setScore(Integer.parseInt(values[Constants.SCORE]));
			
			record.setSummaryWords(Arrays.asList(reviewWords));
			
			record.setUser(values[Constants.USER_ID]);
			
			record.setYear(year);
			
			return record;

		} catch (Exception e) {
			System.err.println("Invalid Date / Line" + e.getMessage());
			return null;
		}
		
	}
	
}
