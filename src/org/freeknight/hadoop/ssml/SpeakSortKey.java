
package org.freeknight.hadoop.ssml;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 自定义MapReduce的Key值, 按统计值从大到小排序.
 * <p>
 * 实现{@link WritableComparable}范型接口, 范型类型为自身.
 * </p>
 * 
 * 
 * @author yrj
 *
 */
public class SpeakSortKey
		/* extends BinaryComparable */
		implements WritableComparable< SpeakSortKey >
{

	private String	speak;

	private int			count;

	public SpeakSortKey ( ) {
	}

	public SpeakSortKey (
			String speak,
			int count ) {
		this.speak = speak;
		this.count = count;
	}

	@Override
	public void readFields (
			DataInput input ) throws IOException
	{
		this.speak = input.readUTF ( );
		this.count = input.readInt ( );
	}

	@Override
	public void write (
			DataOutput output ) throws IOException
	{
		output.writeUTF ( speak );
		output.writeInt ( count );
	}

	@Override
	public String toString ( )
	{
		return speak + ":" + count;
	}

	@Override
	public int hashCode ( )
	{
		return this.speak.hashCode ( ) + this.count;
	}

	@Override
	public boolean equals (
			Object obj )
	{
		if ( !( obj instanceof SpeakSortKey ) )
		{
			return false;
		}
		SpeakSortKey sortKey = ( SpeakSortKey ) obj;
		return ( ( this.speak.equals ( sortKey.speak ) ) && this.count == sortKey.count );
	}

	public String getSpeak ( )
	{
		return speak;
	}

	public void setSpeak (
			String speak )
	{
		this.speak = speak;
	}

	public int getCount ( )
	{
		return count;
	}

	public void setCount (
			int count )
	{
		this.count = count;
	}

	@Override
	public int compareTo (
			SpeakSortKey o )
	{
		return o.getCount ( ) - count;
	}

}
