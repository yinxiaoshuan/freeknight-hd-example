
package org.freeknight.hadoop.api;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

/**
 * 向HDFS写入文件.
 * 
 * @author yrj
 *
 */
public class WriteFile
{

	public static void main (
			String[ ] args )
	{
		write1 ( );
	}

	private static void write1 ( )
	{
		String file = "ssml-info-2017-11-14-1.log";
		String srcFile = "/Users/yrj/Documents/" + file;
		String inPath = "hdfs://192.168.64.129:9000/user/ssml/" + file;

		InputStream input = null;
		FileSystem fs = null;
		try
		{
			input = new FileInputStream ( srcFile );
			fs = FileSystem.get ( URI.create ( inPath ), new Configuration ( ) );
			final FSDataOutputStream fsOutputStream = fs.create ( new Path ( inPath ), new Progressable ( )
			{
				@Override
				public void progress ( )
				{
					System.out.print ( '.' );
				}
			} );
			IOUtils.copyBytes ( input, fsOutputStream, 1024 * 2 );
		}
		catch ( IOException e )
		{
			e.printStackTrace ( );
		}
		finally
		{
			IOUtils.closeStream ( fs );
			IOUtils.closeStream ( input );
		}
	}
}
