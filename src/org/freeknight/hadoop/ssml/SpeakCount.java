
package org.freeknight.hadoop.ssml;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 统计每天的Speak总量及单个总量.
 * 
 * <p>
 * Speak总量为所有单个Speak量之和
 * </p>
 * 
 * @author yrj
 *
 */
public class SpeakCount
{

	public static void main (
			String[ ] args ) throws IOException
	{
		String inputPath = "hdfs://192.168.64.129:9000/user/ssml/*.log";
		String outputPath = "hdfs://192.168.64.129:9000/user/ssml/out";

		Job job = Job.getInstance ( new Configuration ( ), "ssml_speak_count" );
		job.setJarByClass ( SpeakCount.class );
		job.setMapperClass ( SpeakMapper.class );
		job.setMapOutputKeyClass ( Text.class );
		job.setMapOutputValueClass ( IntWritable.class );

		job.setReducerClass ( SpeakReducer.class );

		FileInputFormat.setInputPathFilter ( job, IgnorePathFilter.class );
		FileInputFormat.setInputPaths ( job, new Path ( inputPath ) );
		FileOutputFormat.setOutputPath ( job, new Path ( outputPath ) );

		boolean completion = false;
		try
		{
			completion = job.waitForCompletion ( true );
		}
		catch ( ClassNotFoundException | InterruptedException e )
		{
			e.printStackTrace ( );
		}
		System.exit ( completion ? 0 : 1 );
	}
}
