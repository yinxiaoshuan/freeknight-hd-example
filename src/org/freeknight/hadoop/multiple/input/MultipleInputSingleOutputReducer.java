
package org.freeknight.hadoop.multiple.input;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MultipleInputSingleOutputReducer
		extends Reducer< Text, IntWritable, Text, IntWritable >
{

	@Override
	protected void reduce (
			Text key, Iterable< IntWritable > nums, Reducer< Text, IntWritable, Text, IntWritable >.Context context ) throws IOException,
			InterruptedException
	{
		int sum = 0;
		for ( IntWritable num : nums )
		{
			sum += num.get ( );
		}
		context.write ( key, new IntWritable ( sum ) );
	}
}
