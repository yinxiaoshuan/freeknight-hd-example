
package org.freeknight.hadoop.dedup;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DedupReducer
		extends Reducer< Text, NullWritable, NullWritable, Text >
{

	@Override
	protected void reduce (
			Text key, Iterable< NullWritable > nulls, Reducer< Text, NullWritable, NullWritable, Text >.Context context ) throws IOException,
			InterruptedException
	{
		context.write ( NullWritable.get ( ), key );// Key作为值写入到文件.
	}
}
