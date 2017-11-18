
package org.freeknight.hadoop.flow.foreach;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GroupReducer
		extends Reducer< Text, NullWritable, Text, Text >
{

	@Override
	protected void reduce (
			Text arg0, Iterable< NullWritable > arg1, Reducer< Text, NullWritable, Text, Text >.Context arg2 ) throws IOException, InterruptedException
	{
		arg2.write ( arg0, arg0 );
	}
}
