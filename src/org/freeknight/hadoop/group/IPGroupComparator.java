
package org.freeknight.hadoop.group;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

public class IPGroupComparator
		extends WritableComparator
{

	@Override
	public int compare (
			Object a, Object b )
	{
		System.out.println ( a );
		System.out.println ( b );
		System.out.println ( "=============" );

		Text aText = ( Text ) a;
		Text bText = ( Text ) b;
		return aText.equals ( bText ) ? 0 : -1;
	}

}
