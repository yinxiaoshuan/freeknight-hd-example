
package org.freeknight.hadoop.flow.foreach;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PreAnalysisReducer
		extends Reducer< Text, NullWritable, Text, NullWritable >
{

	@Override
	protected void reduce (
			Text log, Iterable< NullWritable > values, Reducer< Text, NullWritable, Text, NullWritable >.Context context ) throws IOException,
			InterruptedException
	{
		context.write ( log, NullWritable.get ( ) );
	}
}
