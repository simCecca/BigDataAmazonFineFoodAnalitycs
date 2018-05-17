package bigdata.spark.jobs;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import bigdata.spark.Constants;
import bigdata.spark.RecordParser;
import scala.Tuple2;

class StringTupleComparator implements Comparator<Tuple2<String, String>>, Serializable {

	
	private static final long serialVersionUID = 110104234L;

	@Override
	public int compare(Tuple2<String, String> t1, Tuple2<String, String> t2) {

		int cmp = t1._1().compareTo(t2._1);
		if (cmp == 0) cmp = t1._2.compareTo(t2._2);
		return cmp;

	}

}

public class Job3 implements Serializable {

	
	private static final long serialVersionUID = 9898777L;
	private JavaSparkContext sc;

	public void close() {
		sc.close();
	}

	private JavaPairRDD<String, String> loadData(String pathToFile) {
		SparkConf conf = new SparkConf().setAppName("Job3");
		sc = new JavaSparkContext(conf);
		JavaRDD<String> rows = sc.textFile(pathToFile);

		RecordParser parser = new RecordParser();
		JavaPairRDD<String, String> records = rows.map(parser::parseRecord).filter(r -> r != null && r.getYear() >= Constants.MIN_DATE_JOB1)
				.mapToPair(r -> new Tuple2<String, String>(r.getUser(), r.getProduct()));


		return records;

	}

	private void job3Task(String pathToFile) {
		JavaPairRDD<String, String> user2product = loadData(pathToFile)
				.distinct();

		JavaPairRDD<String, Tuple2<String,String>> sameUser = user2product.join(user2product);
		

		JavaPairRDD<Tuple2<String, String>, Integer> counted = sameUser.mapToPair(pair -> new Tuple2<Tuple2<String, String>, Integer>(pair._2, 1))
		.filter(pair -> pair._1._1.compareTo(pair._1._2) < 0)
		.reduceByKey((a, b) -> a + b)
		.sortByKey(new StringTupleComparator());

		counted.saveAsTextFile("job3result");

	}


	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: <filetxt>");
			System.exit(1);
		}

		Job3 job3 = new Job3();

		job3.job3Task(args[0]);
		
		job3.close();

	}
}

