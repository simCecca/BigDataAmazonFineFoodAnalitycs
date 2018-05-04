package bigdata.project1.job2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AnalisysJob2 {
	public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
    	 Job job = new Job(new Configuration(), "AmazonJob2");
         
         job.setJarByClass(AnalisysJob2.class);
         job.setMapperClass(MapperJob2.class);
         job.setReducerClass(ReducerJob2.class);
         
         FileInputFormat.addInputPath(job, new Path(args[0]));
 		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
 		 
 		job.setMapOutputKeyClass(YearProductWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
         
         job.setOutputKeyClass(IntWritable.class);
 		 job.setOutputValueClass(IntWritable.class);
         
 		 job.waitForCompletion(true);
    }
}
