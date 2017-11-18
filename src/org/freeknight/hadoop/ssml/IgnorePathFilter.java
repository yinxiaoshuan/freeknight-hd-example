
package org.freeknight.hadoop.ssml;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class IgnorePathFilter
		implements PathFilter
{

	private String	suffix	= ".log";

	@Override
	public boolean accept (
			Path path )
	{
		String pathName = path.getName ( );
		if ( pathName.isEmpty ( ) )
		{
			return false;
		}

		if ( pathName.endsWith ( suffix ) )
		{
			return true;
		}

		System.err.println ( "Ignore Path: " + path );
		return false;
	}

}
