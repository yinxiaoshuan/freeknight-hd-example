
package org.freeknight.hadoop.flow.foreach;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThreadSortAnalysisMapper
		extends Mapper< Object, Text, ThreadKeyComparable, NullWritable >
{

	@Override
	protected void map (
			Object key, Text value, Mapper< Object, Text, ThreadKeyComparable, NullWritable >.Context context ) throws IOException, InterruptedException
	{
		StringTokenizer tokenizer = new StringTokenizer ( value.toString ( ) );

		ThreadKeyComparable outKey = new ThreadKeyComparable ( );
		outKey.setThread ( tokenizer.nextToken ( ) );
		outKey.setCount ( Integer.parseInt ( tokenizer.nextToken ( ) ) );

		context.write ( outKey, NullWritable.get ( ) );
	}
}
