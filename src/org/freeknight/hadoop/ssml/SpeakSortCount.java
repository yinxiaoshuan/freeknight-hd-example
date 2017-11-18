
package org.freeknight.hadoop.ssml;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SpeakSortCount
{

	public static void main (
			String[ ] args ) throws IOException
	{
		String inputPath = "hdfs://192.168.64.129:9000/user/ssml/out/part-*";
		String outputPath = "hdfs://192.168.64.129:9000/user/ssml/out/sort";

		Job job = Job.getInstance ( new Configuration ( ), "ssml_speak_sort_count" );

		FileInputFormat.addInputPaths ( job, inputPath );
		FileOutputFormat.setOutputPath ( job, new Path ( outputPath ) );

		job.setJarByClass ( SpeakSortCount.class );
		job.setMapperClass ( SpeakSortMapper.class );
		/* job.setMapOutputKeyClass ( Text.class ); */
		job.setMapOutputKeyClass ( SpeakSortKey.class );
		/* job.setMapOutputValueClass ( IntWritable.class ); */
		job.setMapOutputValueClass ( NullWritable.class );

		job.setReducerClass ( SpeakSortReducer.class );

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
