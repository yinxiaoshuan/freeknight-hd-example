
package org.freeknight.hadoop.ssml;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class SpeakSortMapper
		extends Mapper< Object, Text, SpeakSortKey, NullWritable >
{

	@Override
	protected void map (
			Object key, Text value, Mapper< Object, Text, SpeakSortKey, NullWritable >.Context context ) throws IOException, InterruptedException
	{
		String[ ] values = value.toString ( ).split ( "\t" );
		if ( values.length != 2 )
		{
			System.out.println ( value.toString ( ) );
			return;
		}

		SpeakSortKey sortKey = new SpeakSortKey ( );
		sortKey.setSpeak ( values [ 0 ] );
		sortKey.setCount ( Integer.parseInt ( values [ 1 ] ) );
		context.write ( sortKey, NullWritable.get ( ) );
	}
}
