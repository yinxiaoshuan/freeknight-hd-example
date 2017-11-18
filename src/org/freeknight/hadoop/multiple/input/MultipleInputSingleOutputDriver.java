
package org.freeknight.hadoop.multiple.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 多输入、单输出.
 * <p/>
 * 演示多个输入目录, 合并输出到单一文件.
 * 
 * @author yrj
 *
 */
public class MultipleInputSingleOutputDriver
{

	public static void main (
			String[ ] args ) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration ( );

		Job forum_job = Job.getInstance ( conf, MultipleInputSingleOutputDriver.class.getName ( ) + ".job" );
		forum_job.setJarByClass ( MultipleInputSingleOutputDriver.class );
		forum_job.setMapOutputKeyClass ( Text.class );
		forum_job.setMapOutputValueClass ( IntWritable.class );

		forum_job.setReducerClass ( MultipleInputSingleOutputReducer.class );

		/*
		 * 多输入目录情况下, 通过 MultipleInputs.addInputPath方法灵活设置每个目录的输入来源、对应的Mapper处理类.
		 * 
		 * 多想一点: 不同目录可以有不同Mapper, 后续只有一个Reducer.
		 */
		addInputPath ( forum_job );

		// 合并输出.
		String outputPath = "hdfs://192.168.1.100:9000/crawler/output/statistics/merge";
		FileOutputFormat.setOutputPath ( forum_job, new Path ( outputPath ) );

		forum_job.waitForCompletion ( true );
	}

	private static void addInputPath (
			Job forum_job )
	{
		addInputPath (
				"hdfs://192.168.1.100:9000/crawler/forum/*crawler.log",
				forum_job,
				TextInputFormat.class,
				TopicMultipleInputSingleOutputMapper.class );
		addInputPath ( "hdfs://192.168.1.100:9000/crawler/singer/*.log", forum_job, TextInputFormat.class, SingerMultipleInputSingleOutputMapper.class );
	}

	private static void addInputPath (
			String path, Job forum_job, @SuppressWarnings ( "rawtypes" ) Class< ? extends InputFormat > inputFormat,
			Class< ? extends Mapper< ?, ?, ?, ? >> mapper )
	{
		MultipleInputs.addInputPath ( forum_job, new Path ( path ), inputFormat, mapper );
	}
}
