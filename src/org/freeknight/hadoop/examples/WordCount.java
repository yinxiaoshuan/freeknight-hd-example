
package org.freeknight.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount
{

	public static void main (
			String[ ] args ) throws IOException
	{
		Configuration config = new Configuration ( );

		String[ ] _args = new GenericOptionsParser ( config, args ).getRemainingArgs ( );
		if ( _args.length != 2 )
		{
			System.err.println ( "Usage: WordCount <in> <out>" );
			System.exit ( 2 );
		}

		String jobName = "word_count_job";
		Job job = Job.getInstance ( config, jobName );
		job.setJarByClass ( WordCount.class );
		job.setMapperClass ( TokenizerMapper.class );
		job.setCombinerClass ( IntSumReducer.class );
		job.setReducerClass ( IntSumReducer.class );

		job.setOutputKeyClass ( Text.class );
		job.setOutputValueClass ( IntWritable.class );

		FileInputFormat.addInputPath ( job, new Path ( _args [ 0 ] ) );
		FileOutputFormat.setOutputPath ( job, new Path ( _args [ 1 ] ) );

		boolean comple = false;
		try
		{
			comple = job.waitForCompletion ( true );
		}
		catch ( ClassNotFoundException | InterruptedException e )
		{
			e.printStackTrace ( );
		}
		System.exit ( comple ? 0 : 1 );
	}

}
