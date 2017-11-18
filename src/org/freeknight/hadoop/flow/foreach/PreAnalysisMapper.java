
package org.freeknight.hadoop.flow.foreach;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.freeknight.hadoop.flow.Log;

public class PreAnalysisMapper
		extends Mapper< LongWritable, Text, Text, NullWritable >
{

	@Override
	protected void map (
			LongWritable key, Text value, Mapper< LongWritable, Text, Text, NullWritable >.Context context ) throws IOException, InterruptedException
	{
		Log log = Log.doParse ( value.toString ( ) );
		if ( !log.isValid ( ) )
		{
			return;
		}

		context.write ( new Text ( log.toString ( ) ), NullWritable.get ( ) );
	}
}
