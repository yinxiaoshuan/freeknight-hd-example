
package org.freeknight.hadoop.flow.foreach;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ThreadAnalysisReducer
		extends Reducer< Text, IntWritable, Text, IntWritable >
{

	@Override
	protected void reduce (
			Text thread, Iterable< IntWritable > values, Reducer< Text, IntWritable, Text, IntWritable >.Context context ) throws IOException,
			InterruptedException
	{
		int sum = 0;
		for ( IntWritable val : values )
		{
			sum += val.get ( );
		}

		context.write ( thread, new IntWritable ( sum ) );
	}
}
