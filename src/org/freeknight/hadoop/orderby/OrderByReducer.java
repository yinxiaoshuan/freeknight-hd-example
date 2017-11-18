
package org.freeknight.hadoop.orderby;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderByReducer
		extends Reducer< DoubleWritable, Text, IntWritable, Text >
{

	private AtomicInteger	counts	= new AtomicInteger ( 0 );

	@Override
	protected void reduce (
			DoubleWritable arg0, Iterable< Text > arg1, Reducer< DoubleWritable, Text, IntWritable, Text >.Context arg2 ) throws IOException,
			InterruptedException
	{
		for ( Text arg : arg1 )
		{
			arg2.write ( new IntWritable ( counts.incrementAndGet ( ) ), arg );
		}
	}
}
