
package org.freeknight.hadoop.group;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.freeknight.framework.helper.GsonConverter;
import org.freeknight.hadoop.flow.Log;
import org.freeknight.hadoop.flow.Speak;

public class GroupMapper
		extends Mapper< Object, Text, Text, NullWritable >
{

	@Override
	protected void map (
			Object key, Text value, Mapper< Object, Text, Text, NullWritable >.Context context ) throws IOException, InterruptedException
	{
		Log log = GsonConverter.toBean ( value.toString ( ), Log.class );
		Speak speak = log.getSpeak ( );

		Text outPutKey = new Text ( log.getThread ( ) + "-" + speak.getRemote_host ( ) );
		context.write ( outPutKey, NullWritable.get ( ) );
	}
}
