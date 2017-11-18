
package org.freeknight.hadoop.dedup;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DedupDriver
{

	public static void main (
			String[ ] args ) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration ( );
		String jobName = DedupDriver.class.getCanonicalName ( ) + ".job";
		Job job = Job.getInstance ( conf, jobName );

		job.setJarByClass ( DedupDriver.class );
		job.setMapperClass ( DedupMapper.class );
		job.setReducerClass ( DedupReducer.class );

		// job.setOutputKeyClass ( NullWritable.class );
		// job.setOutputValueClass ( Text.class );
		job.setMapOutputKeyClass ( Text.class );
		job.setMapOutputValueClass ( NullWritable.class );

		String inputPath = "hdfs://192.168.1.100:9000/user/dedup/dedup-*.txt";
		String outPath = "hdfs://192.168.1.100:9000/user/dedup/output";

		FileInputFormat.addInputPath ( job, new Path ( inputPath ) );
		FileOutputFormat.setOutputPath ( job, new Path ( outPath ) );

		System.exit ( job.waitForCompletion ( true ) ? 0 : 1 );
	}
}
