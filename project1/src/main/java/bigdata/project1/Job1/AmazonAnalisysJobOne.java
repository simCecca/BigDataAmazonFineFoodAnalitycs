package bigdata.project1.Job1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Hello world!
 *
 */
public class AmazonAnalisysJobOne 
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
    	 Job job = new Job(new Configuration(), "Amazon");
         
    	 long start = System.currentTimeMillis();
    	 
         job.setJarByClass(AmazonAnalisysJobOne.class);
         job.setMapperClass(MapperJob1.class);
         job.setReducerClass(ReducerJob1.class);
         
         FileInputFormat.addInputPath(job, new Path(args[0]));
 		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
         
         job.setOutputKeyClass(IntWritable.class);
 		 job.setOutputValueClass(Text.class);
         
 		 job.waitForCompletion(true);
 		 
 		 long end = System.currentTimeMillis();
		
 		 long time = end - start;
 		 
		
 		 System.out.println("\n\n\n\n\n\n\n");
 		 System.out.println("TEMPO:    " + time/1000.0);
 		System.out.println("\n\n\n\n\n\n\n");
    }
}
