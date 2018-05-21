package bigdata.project1.job3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AnalisysJob3 {
	
	public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
	{
		Job job = new Job(new Configuration(), "AmazonJob3");

		long startTime = System.currentTimeMillis();
		
		job.setJarByClass(AnalisysJob3.class);
		job.setMapperClass(MapperPart1Job3.class);
		job.setReducerClass(ReducerPart1Job3.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

		long endFirst = System.currentTimeMillis();

		Job job2 = new Job(new Configuration(), "AmazonJob3.2");
		job2.setJarByClass(AnalisysJob3.class);
		job2.setMapperClass(MapperPart2Job3.class);
		job2.setReducerClass(ReducerPart2Job3.class);

		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		job2.waitForCompletion(true);

		long endSecond = System.currentTimeMillis();
		
		long timeJob1 = endFirst - startTime;
		long timeJob2 = endSecond - endFirst;
		
		System.out.println("\n\n\n\n\n\n\n");
		System.out.println("TEMPO PRIMO JOB:    " + timeJob1/1000.0 + "              TEMPO SECONDO JOB:    " + timeJob2/1000.0);
	
	}

}
