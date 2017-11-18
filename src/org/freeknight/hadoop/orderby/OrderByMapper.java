
package org.freeknight.hadoop.orderby;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderByMapper
		extends Mapper< LongWritable, Text, DoubleWritable, Text >
{

	@Override
	protected void map (
			LongWritable key, Text value, Mapper< LongWritable, Text, DoubleWritable, Text >.Context context ) throws IOException, InterruptedException
	{
		String[ ] items = value.toString ( ).split ( "\t" );
		if ( items == null || items.length != 2 )
		{
			context.getCounter ( LineCounter.ERROR_LINE ).increment ( 1 );
		}

		System.out.println ( items [ 2 ] );

		context.getCounter ( LineCounter.TOTAL ).increment ( 1 );

		context.write ( new DoubleWritable ( Double.parseDouble ( items [ 2 ] ) ), value );
	}
}
