
package org.freeknight.hadoop.multiple.output;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultipleMapper
		extends Mapper< LongWritable, Text, Text, IntWritable >
{
	@Override
	protected void map (
			LongWritable key, Text value, Mapper< LongWritable, Text, Text, IntWritable >.Context context ) throws IOException, InterruptedException
	{
		String[ ] ips = value.toString ( ).split ( ":" );
		if ( ips.length != 2 )
		{
			return;
		}

		context.write ( new Text ( ips [ 1 ] ), new IntWritable ( 1 ) );
	}
}
