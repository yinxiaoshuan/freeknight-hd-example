
package org.freeknight.hadoop.flow.foreach;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.freeknight.framework.helper.GsonConverter;
import org.freeknight.hadoop.flow.Log;

public class ThreadAnalysisMapper
		extends Mapper< LongWritable, Text, Text, IntWritable >
{

	@Override
	protected void map (
			LongWritable key, Text value, Mapper< LongWritable, Text, Text, IntWritable >.Context context ) throws IOException, InterruptedException
	{
		Log log = GsonConverter.toBean ( value.toString ( ), Log.class );
		String thread = log.getThread ( );

		context.write ( new Text ( thread ), new IntWritable ( 1 ) );
	}
}
