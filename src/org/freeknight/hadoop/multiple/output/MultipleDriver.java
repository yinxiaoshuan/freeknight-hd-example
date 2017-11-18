
package org.freeknight.hadoop.multiple.output;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultipleDriver
{

	public static void main (
			String[ ] args ) throws IOException
	{
		Configuration config = new Configuration ( );

		Job ipAnalysisJob = Job.getInstance ( config, MultipleDriver.class.getName ( ) + ".job" );
		ipAnalysisJob.setJarByClass ( MultipleDriver.class );

		ipAnalysisJob.setMapperClass ( MultipleMapper.class );
		ipAnalysisJob.setMapOutputKeyClass ( Text.class );
		ipAnalysisJob.setMapOutputValueClass ( IntWritable.class );

		ipAnalysisJob.setReducerClass ( MultipleReducer.class );
		// ipAnalysisJob.setNumReduceTasks ( 1 );
		// ipAnalysisJob.setMaxMapAttempts ( 1 );
		// ipAnalysisJob.setMaxReduceAttempts ( 1 );

		FileInputFormat.addInputPath ( ipAnalysisJob, new Path ( "hdfs://192.168.64.129:9000/user/ip_host/ip-*" ) );

		addNamedOutput ( ipAnalysisJob );// 设置多目录输出.
		FileOutputFormat.setOutputPath ( ipAnalysisJob, new Path ( "hdfs://192.168.64.129:9000/user/ip_host/output" ) );

		try
		{
			ipAnalysisJob.waitForCompletion ( true );
		}
		catch ( ClassNotFoundException | InterruptedException e )
		{
			e.printStackTrace ( );
		}
	}

	public static void addNamedOutput (
			Job ipAnalysisJob )
	{
		addNamedOutput ( ipAnalysisJob, "A" );
		addNamedOutput ( ipAnalysisJob, "B" );
		addNamedOutput ( ipAnalysisJob, "C" );
		addNamedOutput ( ipAnalysisJob, "D" );
		addNamedOutput ( ipAnalysisJob, "E" );
		addNamedOutput ( ipAnalysisJob, "F" );
		addNamedOutput ( ipAnalysisJob, "G" );
		addNamedOutput ( ipAnalysisJob, "H" );
		addNamedOutput ( ipAnalysisJob, "I" );
		addNamedOutput ( ipAnalysisJob, "J" );
		addNamedOutput ( ipAnalysisJob, "K" );
		addNamedOutput ( ipAnalysisJob, "L" );
		addNamedOutput ( ipAnalysisJob, "M" );
		addNamedOutput ( ipAnalysisJob, "N" );
		addNamedOutput ( ipAnalysisJob, "O" );
		addNamedOutput ( ipAnalysisJob, "P" );
		addNamedOutput ( ipAnalysisJob, "Q" );
		addNamedOutput ( ipAnalysisJob, "R" );
		addNamedOutput ( ipAnalysisJob, "S" );
		addNamedOutput ( ipAnalysisJob, "T" );
		addNamedOutput ( ipAnalysisJob, "U" );
		addNamedOutput ( ipAnalysisJob, "V" );
		addNamedOutput ( ipAnalysisJob, "W" );
		addNamedOutput ( ipAnalysisJob, "X" );
		addNamedOutput ( ipAnalysisJob, "Y" );
		addNamedOutput ( ipAnalysisJob, "Z" );
	}

	private static void addNamedOutput (
			Job job, String output )
	{
		MultipleOutputs.addNamedOutput ( job, output, FileOutputFormat.class, Text.class, IntWritable.class );
	}
}
