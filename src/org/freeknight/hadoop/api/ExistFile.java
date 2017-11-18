
package org.freeknight.hadoop.api;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class ExistFile
{

	public static void main (
			String[ ] args )
	{
		boolean ex = exist ( new Configuration ( ), "hdfs://192.168.64.129:9000/user/ssml/ssml-info-2017-11-14-1.log" );
		System.out.println ( ex );
	}

	public static boolean exist (
			Configuration conf, String path )
	{
		boolean exists = false;
		FileSystem fs = null;
		try
		{
			fs = FileSystem.get ( URI.create ( path ), conf );
			exists = fs.exists ( new Path ( path ) );
		}
		catch ( IOException e )
		{
			e.printStackTrace ( );
		}
		finally
		{
			IOUtils.closeStream ( fs );
		}
		return exists;
	}
}
