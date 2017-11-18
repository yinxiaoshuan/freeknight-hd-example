
package org.freeknight.hadoop.multiple.input;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultipleInputMultipleOutputReducer
		extends Reducer< MulMulKeyPair, IntWritable, Text, IntWritable >
{

	private MultipleOutputs< Text, IntWritable >	outputs;

	@Override
	protected void setup (
			Reducer< MulMulKeyPair, IntWritable, Text, IntWritable >.Context context ) throws IOException, InterruptedException
	{
		outputs = new MultipleOutputs< Text, IntWritable > ( context );
	}

	@Override
	protected void reduce (
			MulMulKeyPair key, Iterable< IntWritable > nums, Reducer< MulMulKeyPair, IntWritable, Text, IntWritable >.Context context ) throws IOException,
			InterruptedException
	{
		int sum = 0;
		for ( IntWritable num : nums )
		{
			sum += num.get ( );
		}

		outputs.write ( new Text ( key.getName ( ) ), new IntWritable ( sum ), key.getNamedOutput ( ) );
	}

	@Override
	protected void cleanup (
			Reducer< MulMulKeyPair, IntWritable, Text, IntWritable >.Context context ) throws IOException, InterruptedException
	{
		outputs.close ( );
		super.cleanup ( context );
	}
}
