
package org.freeknight.hadoop.flow.foreach;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 迭代式MapReduce作业示例代码.
 * 
 * @author yrj
 *
 */
public class IterationDriver
{

	private final static String	INPUT_PATH	= "hdfs://192.168.64.129:9000/user/ssml/ssml-info-2017-11-04-1.log";

	private final static String	OUTPUT_PATH	= "hdfs://192.168.64.129:9000/user/ssml/output";

	public static void main (
			String[ ] args ) throws IOException
	{
		Configuration config = new Configuration ( );

		// 第一个作业：pre_analysis_job
		Job analysis_job = Job.getInstance ( config, "pre_analysis_job" );
		analysis_job.setJarByClass ( IterationDriver.class );
		analysis_job.setOutputKeyClass ( Text.class );
		analysis_job.setOutputValueClass ( Text.class );

		analysis_job.setMapperClass ( PreAnalysisMapper.class );
		analysis_job.setMapOutputKeyClass ( Text.class );
		analysis_job.setMapOutputValueClass ( NullWritable.class );

		analysis_job.setReducerClass ( PreAnalysisReducer.class );

		FileInputFormat.setInputPaths ( analysis_job, new Path ( INPUT_PATH ) );
		FileOutputFormat.setOutputPath ( analysis_job, new Path ( OUTPUT_PATH ) );

		boolean completion = false;
		try
		{
			completion = analysis_job.waitForCompletion ( true );
		}
		catch ( ClassNotFoundException | InterruptedException e )
		{
			e.printStackTrace ( );
		}

		// 第二个作业：thread_analysis_job
		Job thread_job = Job.getInstance ( config, "thread_analysis_job" );
		thread_job.setJarByClass ( IterationDriver.class );
		thread_job.setMapperClass ( ThreadAnalysisMapper.class );
		thread_job.setMapOutputKeyClass ( Text.class );
		thread_job.setMapOutputValueClass ( IntWritable.class );

		thread_job.setReducerClass ( ThreadAnalysisReducer.class );

		/*
		 * 注意: 当前作业的输入为上一个输出目录.
		 */
		FileInputFormat.setInputPaths ( thread_job, FileOutputFormat.getOutputPath ( analysis_job ) );
		FileOutputFormat.setOutputPath ( thread_job, new Path ( OUTPUT_PATH + "/thread" ) );

		try
		{
			completion = thread_job.waitForCompletion ( true );
		}
		catch ( ClassNotFoundException | InterruptedException e )
		{
			e.printStackTrace ( );
		}

		// 第三个作业：sort_analysis_job
		Job sort_job = Job.getInstance ( config, "sort_analysis_job" );
		sort_job.setJarByClass ( IterationDriver.class );
		sort_job.setMapperClass ( ThreadSortAnalysisMapper.class );
		sort_job.setMapOutputKeyClass ( ThreadKeyComparable.class );
		sort_job.setMapOutputValueClass ( NullWritable.class );

		/*
		 * 注意: 当前作业的输入为上一个输出目录.
		 */
		FileInputFormat.setInputPaths ( sort_job, FileOutputFormat.getOutputPath ( thread_job ) );
		FileOutputFormat.setOutputPath ( sort_job, new Path ( OUTPUT_PATH + "/thread/statistics" ) );

		try
		{
			completion = sort_job.waitForCompletion ( true );
		}
		catch ( ClassNotFoundException | InterruptedException e )
		{
			e.printStackTrace ( );
		}

		System.exit ( completion ? 0 : 1 );
	}
}
