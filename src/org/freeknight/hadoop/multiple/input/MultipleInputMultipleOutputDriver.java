
package org.freeknight.hadoop.multiple.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 多输入、多输出.
 * <p/>
 * 演示多输入目录对应多个输出目录.
 * 
 * @author yrj
 *
 */
public class MultipleInputMultipleOutputDriver
{
	public static void main (
			String[ ] args ) throws ClassNotFoundException, IOException, InterruptedException
	{
		Configuration conf = new Configuration ( );

		Job forum_job = Job.getInstance ( conf, MultipleInputMultipleOutputDriver.class.getName ( ) + ".job" );
		forum_job.setJarByClass ( MultipleInputMultipleOutputDriver.class );
		forum_job.setMapOutputKeyClass ( MulMulKeyPair.class );
		forum_job.setMapOutputValueClass ( IntWritable.class );

		forum_job.setReducerClass ( MultipleInputMultipleOutputReducer.class );

		// 自定义分区及数量.
		// 设置Reduce数量, 决定输出文件分区个数. 默认分区个数为Reduce数量, 默认为1.
		// 默认情况下, 输出文件格式为: part-r-00000.
		// 当指定NumReduceTasks为n个, 则输出文件从 00000开始, 到n-1. 比如 n = 3, 则文件为:part-r-00000、part-r-00001、part-r-00002.
		// 按找Key进行分区, 具体哪个Key写入到哪个文件, 依据KeyFieldBasedPartitioner生成.
		// TODO 分区参考: http://blog.csdn.net/yuan_xw/article/details/50867819、https://www.cnblogs.com/luoliang/p/4184938.html
		forum_job.setNumReduceTasks ( 3 );
		// forum_job.setPartitionerClass ( RemainderPartitioner.class );

		// 多输入
		addInputPath ( forum_job );
		// 合并输出.
		addOutputPath ( forum_job );
		String outputPath = "hdfs://192.168.64.129:9000/crawler/output/statistics/multiple";
		FileOutputFormat.setOutputPath ( forum_job, new Path ( outputPath ) );

		forum_job.waitForCompletion ( true );
	}

	// InputPath
	private static void addInputPath (
			Job forum_job )
	{
		addInputPath (
				"hdfs://192.168.64.129:9000/crawler/forum/*crawler.log",
				forum_job,
				TextInputFormat.class,
				TopicMultipleInputMultipleOutputMapper.class );
		addInputPath ( "hdfs://192.168.64.129:9000/crawler/singer/*.log", forum_job, TextInputFormat.class, SingerMultipleInputMultipleOutputMapper.class );
	}

	private static void addInputPath (
			String path, Job forum_job, @SuppressWarnings ( "rawtypes" ) Class< ? extends InputFormat > inputFormat,
			Class< ? extends Mapper< ?, ?, ?, ? >> mapper )
	{
		MultipleInputs.addInputPath ( forum_job, new Path ( path ), inputFormat, mapper );
	}

	// OutputPath
	private static void addOutputPath (
			Job forum_job )
	{
		addOutputPath ( TopicMultipleInputMultipleOutputMapper.TOPIC_NAMED, forum_job, TextOutputFormat.class, MulMulKeyPair.class, IntWritable.class );
		addOutputPath ( SingerMultipleInputMultipleOutputMapper.SINGER_NAMED, forum_job, TextOutputFormat.class, MulMulKeyPair.class, IntWritable.class );
	}

	private static void addOutputPath (
			String namedOutput, Job job, @SuppressWarnings ( "rawtypes" ) Class< TextOutputFormat > outputFormatClass, Class< ? > keyClass,
			Class< ? > valueClass )
	{
		MultipleOutputs.addNamedOutput ( job, namedOutput, outputFormatClass, keyClass, valueClass );
	}
}
