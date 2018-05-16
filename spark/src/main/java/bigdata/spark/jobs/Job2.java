package bigdata.spark.jobs;

import java.io.Serializable;
import java.util.Comparator;
import java.util.stream.StreamSupport;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import bigdata.spark.Constants;
import bigdata.spark.Record;
import bigdata.spark.RecordParser;
import scala.Tuple2;

class TupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {

	private static final long serialVersionUID = 110104234L;

	@Override
	public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {

		int cmp = t1._1().compareTo(t2._1);
		if (cmp == 0) cmp = t1._2 - t2._2;
		return cmp;

	}

}

public class Job2 implements Serializable {
	
	
	
	private static final long serialVersionUID = 176876875L;
	private JavaSparkContext sc;

	public void close() {
		this.sc.close();
	}


	private JavaRDD<Record> loadData(String pathToFile) {
		SparkConf conf = new SparkConf().setAppName("Wordcount");
		sc = new JavaSparkContext(conf);
		JavaRDD<String> rows = sc.textFile(pathToFile);

		RecordParser parser = new RecordParser();
		JavaRDD<Record> records = rows.map(parser::parseRecord).filter(r -> r != null && r.getYear() >= Constants.MIN_DATE_JOB2);


		return records;

	}

	private void job2Task(String pathToFile) {
		JavaRDD<Record> records = loadData(pathToFile);

		JavaPairRDD<Tuple2<String, Integer>, Iterable<Record>> prodYear2record = records.groupBy(r -> new Tuple2<String, Integer>(r.getProduct(), r.getYear()));

		JavaPairRDD<Tuple2<String, Integer>, Double> prodYear2avg = prodYear2record.mapToPair(tuple -> {
			double avg = StreamSupport.stream(tuple._2.spliterator(), false).mapToInt(Record::getScore).average().getAsDouble();

			return new Tuple2<Tuple2<String, Integer>, Double>(tuple._1, avg);
		});
		
		JavaPairRDD<String, Iterable<Tuple2<Integer, Double>>> result = prodYear2avg
				.mapToPair(tuple -> new Tuple2<String, Tuple2<Integer, Double>>(tuple._1._1,new Tuple2<Integer, Double>(tuple._1._2, tuple._2)))
				.groupByKey()
				.sortByKey();
		

		result.coalesce(1).saveAsTextFile("job2result");

	}



	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: <filetxt>");
			System.exit(1);
		}

		Job2 job2 = new Job2();

		job2.job2Task(args[0]);
		
		job2.close();

	}
}

