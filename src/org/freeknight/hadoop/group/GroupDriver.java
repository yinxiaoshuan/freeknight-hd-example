
package org.freeknight.hadoop.group;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.freeknight.hadoop.flow.foreach.GroupReducer;

/**
 * 模拟GroupBy.
 * 
 * @author yrj
 *
 */
public class GroupDriver
{

	public static void main (
			String[ ] args ) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration config = new Configuration ( );

		Job group_job = Job.getInstance ( config, "group_comparator_job" );
		group_job.setJarByClass ( GroupDriver.class );

		group_job.setMapperClass ( GroupMapper.class );
		group_job.setMapOutputKeyClass ( Text.class );
		group_job.setMapOutputValueClass ( NullWritable.class );

		group_job.setReducerClass ( GroupReducer.class );
		group_job.setGroupingComparatorClass ( IPGroupComparator.class );

		FileInputFormat.addInputPath ( group_job, new Path ( "hdfs://192.168.64.129:9000/user/ssml/output/part-*" ) );
		FileOutputFormat.setOutputPath ( group_job, new Path ( "hdfs://192.168.64.129:9000/user/ssml/output/group" ) );

		group_job.waitForCompletion ( true );
	}
}
