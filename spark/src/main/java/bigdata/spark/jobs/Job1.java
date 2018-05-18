package bigdata.spark.jobs;

import java.util.stream.Collectors;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import bigdata.spark.Constants;
import bigdata.spark.Record;
import bigdata.spark.RecordParser;
import scala.Tuple2;

public class Job1 implements Serializable {
	
	

	private static final long serialVersionUID = 13423423423L;


	private Iterable<Tuple2<Integer, String>> getTop10(Iterable<Tuple2<Integer, String>> arg) {
		List<Tuple2<Integer, String>> top10 = new ArrayList<>();
		
		int count = 0;
		for (Tuple2<Integer, String> elem : arg) {
			if (count == 10) break;
			
			top10.add(elem);
			count++;
		}
		
		return top10;
	}
	
	private JavaRDD<Record> loadData(String pathToFile) {
		SparkConf conf = new SparkConf().setAppName("SparkJob2");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> rows = sc.textFile(pathToFile);

		RecordParser parser = new RecordParser();
		JavaRDD<Record> records = rows.map(parser::parseRecord).filter(r -> r != null && r.getYear() >= Constants.MIN_DATE_JOB1);
		
		return records;

	}

	private void job1Task(String pathToFile) {
		JavaRDD<Record> records = loadData(pathToFile);

		JavaPairRDD<Integer, Iterable<Tuple2<Integer, String>>> top10byYear = records.flatMapToPair(r -> r.getSummaryWords().stream()
				.map( (String w) -> {
					Tuple2<Integer, String> key = new Tuple2<>(r.getYear(), w);
					
					return new Tuple2<Tuple2<Integer, String>, Integer>(key, 1);
				}).collect(Collectors.toList()).iterator())
				.reduceByKey((a, b) -> a + b)
				.mapToPair(pair -> new Tuple2<>(pair._2, pair._1))
				.sortByKey(false)
				.mapToPair(pair -> new Tuple2<>(pair._2._1, new Tuple2<>(pair._1, pair._2._2)))
				.groupByKey()
				.sortByKey()
				.mapToPair(pair -> new Tuple2<>(pair._1, getTop10(pair._2)));
				
		top10byYear.coalesce(1).saveAsTextFile("job1result");


	}



	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: <filetxt>");
			System.exit(1);
		}

		Job1 job1 = new Job1();
		
		long start = System.currentTimeMillis();
		job1.job1Task(args[0]);
		long end = System.currentTimeMillis();
		
		System.out.println("\n\n\n\n\n\n\n");
		System.out.println("TEMPO PRIMO JOB:    " + (end-start)/1000.0) ;

	}
}
