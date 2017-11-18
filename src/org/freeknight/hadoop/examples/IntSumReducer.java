
package org.freeknight.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class IntSumReducer
		extends Reducer< Text, IntWritable, Text, IntWritable >
{

	private IntWritable	result	= new IntWritable ( 0 );

	@Override
	protected void reduce (
			Text key, Iterable< IntWritable > values, Reducer< Text, IntWritable, Text, IntWritable >.Context context ) throws IOException,
			InterruptedException
	{
		int sum = 0;
		for ( IntWritable val : values )
		{
			sum += val.get ( );
		}

		result.set ( sum );
		context.write ( key, result );
	}
}
