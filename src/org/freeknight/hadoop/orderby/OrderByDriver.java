
package org.freeknight.hadoop.orderby;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.freeknight.hadoop.api.DeleteFile;

public class OrderByDriver
		extends Configured
		implements Tool
{

	public static void main (
			String[ ] args ) throws IOException, ClassNotFoundException, InterruptedException
	{
		ToolRunner.printGenericCommandUsage ( System.out );

		int exitCode = 0;
		try
		{
			exitCode = ToolRunner.run ( new OrderByDriver ( ), args );
		}
		catch ( Exception e )
		{
			exitCode = -1;
		}
		System.exit ( exitCode );
	}

	@Override
	public int run (
			String[ ] args ) throws Exception
	{
		Configuration conf = new Configuration ( );
		Job job = Job.getInstance ( conf, OrderByDriver.class.getCanonicalName ( ) + ".job" );
		job.setJarByClass ( OrderByDriver.class );

		job.setMapperClass ( OrderByMapper.class );
		job.setReducerClass ( OrderByReducer.class );

		job.setMapOutputKeyClass ( DoubleWritable.class );
		job.setMapOutputValueClass ( Text.class );

		String inputPath = "hdfs://192.168.1.100:9000/user/orderby/orderby.txt";
		String outputPath = "hdfs://192.168.1.100:9000/user/orderby/output";
		DeleteFile.deletePath ( conf, outputPath );

		FileInputFormat.addInputPath ( job, new Path ( inputPath ) );
		FileOutputFormat.setOutputPath ( job, new Path ( outputPath ) );

		boolean compled = job.waitForCompletion ( true );
		if ( compled )// MapReduce计数器.
		{
			long total = job.getCounters ( ).findCounter ( LineCounter.TOTAL ).getValue ( );
			long error = job.getCounters ( ).findCounter ( LineCounter.ERROR_LINE ).getValue ( );// 错误的记录数, 后续逻辑如何处理视业务决定.
			System.out.println ( "Total: " + total + ", error: " + error );
		}
		return compled ? 0 : 1;
	}
}
