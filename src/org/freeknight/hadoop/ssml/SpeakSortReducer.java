
package org.freeknight.hadoop.ssml;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class SpeakSortReducer
		extends Reducer< SpeakSortKey, NullWritable, Text, NullWritable >
{

	@Override
	protected void reduce (
			SpeakSortKey sortKey, Iterable< NullWritable > values, Reducer< SpeakSortKey, NullWritable, Text, NullWritable >.Context context )
			throws IOException, InterruptedException
	{
		context.write ( new Text ( sortKey.toString ( ) ), NullWritable.get ( ) );
	}

}
