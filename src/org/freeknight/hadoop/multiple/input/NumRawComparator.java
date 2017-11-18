
package org.freeknight.hadoop.multiple.input;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;

public class NumRawComparator
		implements RawComparator< Text >
{

	@Override
	public int compare (
			Text o1, Text o2 )
	{
		return 0;
	}

	@Override
	public int compare (
			byte[ ] arg0, int arg1, int arg2, byte[ ] arg3, int arg4, int arg5 )
	{
		return 0;
	}

}
