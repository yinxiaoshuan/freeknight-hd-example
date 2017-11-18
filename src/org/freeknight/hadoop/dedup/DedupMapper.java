
package org.freeknight.hadoop.dedup;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DedupMapper
		extends Mapper< LongWritable, Text, Text, NullWritable >
{

	@Override
	protected void map (
			LongWritable key, Text value, Mapper< LongWritable, Text, Text, NullWritable >.Context context ) throws IOException, InterruptedException
	{
		Text outKey = value;
		context.write ( outKey, NullWritable.get ( ) );// 值作为Key发送到Reducer, 经过Shuffle聚集、排序形成<Text, NullWrite[]>
	}
}
