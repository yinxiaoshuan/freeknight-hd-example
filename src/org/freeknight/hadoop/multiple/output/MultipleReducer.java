
package org.freeknight.hadoop.multiple.output;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultipleReducer
		extends Reducer< Text, IntWritable, Text, IntWritable >
{

	private MultipleOutputs< Text, IntWritable >	outputs;

	@Override
	protected void setup (
			Reducer< Text, IntWritable, Text, IntWritable >.Context context ) throws IOException, InterruptedException
	{
		super.setup ( context );
		outputs = new MultipleOutputs< Text, IntWritable > ( context );
	}

	@Override
	protected void reduce (
			Text key, Iterable< IntWritable > values, Reducer< Text, IntWritable, Text, IntWritable >.Context context ) throws IOException,
			InterruptedException
	{
		int sum = 0;
		for ( IntWritable val : values )
		{
			sum += val.get ( );
		}

		// 指定输出文件前缀.
		// String prefix = String.valueOf ( key.toString ( ).charAt ( 0 ) );

		char prefix = Character.toUpperCase ( key.toString ( ).charAt ( 0 ) );
		// outputs.write ( String.valueOf ( prefix ), key, new IntWritable ( sum ) );// 此句抛异常, 不是太明白.
		outputs.write ( key, new IntWritable ( sum ), String.valueOf ( prefix ) );
	}

	@Override
	protected void cleanup (
			Reducer< Text, IntWritable, Text, IntWritable >.Context context ) throws IOException, InterruptedException
	{
		this.outputs.close ( );// 关闭.
		super.setup ( context );
	}
}
