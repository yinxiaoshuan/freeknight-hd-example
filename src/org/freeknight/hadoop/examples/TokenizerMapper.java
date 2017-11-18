
package org.freeknight.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class TokenizerMapper
		extends Mapper< Object, Text, Text, IntWritable >
{

	private Text											word	= new Text ( );

	private final static IntWritable	one		= new IntWritable ( 1 );

	@Override
	protected void map (
			Object key, Text value, Mapper< Object, Text, Text, IntWritable >.Context context ) throws IOException, InterruptedException
	{
		StringTokenizer tokenizer = new StringTokenizer ( value.toString ( ) );
		while ( tokenizer.hasMoreTokens ( ) )
		{
			word.set ( tokenizer.nextToken ( ) );
			context.write ( word, one );
		}
	}
}
