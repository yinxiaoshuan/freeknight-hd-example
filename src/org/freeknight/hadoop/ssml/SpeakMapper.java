
package org.freeknight.hadoop.ssml;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SpeakMapper
		extends Mapper< Object, Text, Text, IntWritable >
{

	private IntWritable	count	= new IntWritable ( 1 );

	@Override
	protected void map (
			Object key, Text line, Mapper< Object, Text, Text, IntWritable >.Context context ) throws IOException, InterruptedException
	{
		LogRecord record = LogRecord.parse ( line.toString ( ) );
		if ( !record.isLawful ( ) )
		{
			// System.err.println ( "Discard Line: " + record );
			return;
		}

		Speak speak = record.getSpeak ( );
		System.out.println ( "remote: " + speak.getRemote_host ( ) + ", time: " + speak.getTimeuse ( ) + ", speak: " + speak.getSpeaker ( ) );
		context.write ( new Text ( speak.getSpeaker ( ) ), count );
	}
}
