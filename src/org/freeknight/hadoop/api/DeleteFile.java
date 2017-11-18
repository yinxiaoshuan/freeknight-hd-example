
package org.freeknight.hadoop.api;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class DeleteFile
{

	public static void main (
			String[ ] args )
	{
		Configuration conf = new Configuration ( );
		String inPath = "hdfs://192.168.64.129:9000/user/ssml/ssml-info-2017-11-14-11.log";

		boolean del = deletePath ( conf, inPath );
		System.out.println ( del );
	}

	public static boolean deletePath (
			Configuration conf, String path )
	{
		boolean del = false;
		FileSystem fs = null;
		try
		{
			fs = FileSystem.get ( URI.create ( path ), conf );
			del = fs.deleteOnExit ( new Path ( path ) );
		}
		catch ( IOException e )
		{
			e.printStackTrace ( );
		}
		finally
		{
			IOUtils.closeStream ( fs );
		}
		return del;
	}

}
