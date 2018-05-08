package bigdata.spark.job2;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import bigdata.spark.Constants;
import bigdata.spark.Record;
import bigdata.spark.RecordParser;
import scala.Tuple2;

public class Job1 {
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

		JavaPairRDD<Tuple2<Integer, String>, Integer> yearWord2count = records.flatMapToPair(r -> r.getSummaryWords().stream()
				.map( (String w) -> {
					Tuple2<Integer, String> key = new Tuple2<>(r.getYear(), w);
					
					return new Tuple2<Tuple2<Integer, String>, Integer>(key, 1);
				}).collect(Collectors.toList()))
				.reduceByKey((a, b) -> a + b);
		
		JavaPairRDD<Tuple2<Integer, Integer>, String> yearFreq2word = yearWord2count.mapToPair(pair -> {
			Tuple2<Integer, String> k = pair._1;
			Integer v = pair._2;
			
			Tuple2<Integer, Integer> yearFreqKey = new Tuple2<>(k._1, v);
			
			return new Tuple2<Tuple2<Integer, Integer>, String>(yearFreqKey, k._2);
		}).
	

		JavaPairRDD<Tuple2<String, Integer>, Double> prodYear2avg = prodYear2record.mapToPair(tuple -> {
			double avg = StreamSupport.stream(tuple._2.spliterator(), false).mapToInt(Record::getScore).average().getAsDouble();

			return new Tuple2<Tuple2<String, Integer>, Double>(tuple._1, avg);
		});

		prodYear2avg = prodYear2avg.sortByKey(new TupleComparator());


		prodYear2avg.saveAsTextFile("tu_zia.txt");

	}


	//	private static String pathToFile;
	//
	//	public WordCount(String file){
	//		this.pathToFile = file;
	//	}
	//	/**
	//	 * Load the data from the text file and return an RDD of words
	//	 */
	//	public JavaRDD<String> loadData() {
	//		
	//		JavaRDD<String> words = sc.textFile(pathToFile).flatMap(line -> Arrays.asList(line.split(" ")));
	//		return words;
	//	}
	//
	//	public JavaPairRDD<String, Integer> wordcount() {
	//		JavaRDD<String> words = loadData();
	//		// Step 1: mapper step
	//		JavaPairRDD<String, Integer> couples =
	//				words.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
	//		// Step 2: reducer step
	//		JavaPairRDD<String, Integer> result = couples.reduceByKey((a, b) -> a + b);
	//		return result;
	//	}
	//	public JavaPairRDD<String, Integer> filterOnWordcount(int x) {
	//		JavaPairRDD<String, Integer> wordcounts = wordcount();
	//		JavaPairRDD<String, Integer> filtered =
	//				wordcounts.filter(couple -> couple._2() > x);
	//		return filtered;
	//	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: <filetxt>");
			System.exit(1);
		}

		Job2 job2 = new Job2();

		job2.job2Task(args[0]);

		//WordCount wc = new WordCount(args[0]);
		//System.out.println("wordcount: "+wc.wordcount().toString());
	}
}
