
package org.freeknight.hadoop.multiple.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MulMulKeyPair
		implements WritableComparable< MulMulKeyPair >
{

	private String	namedOutput;

	private String	name;

	private int			total;

	public MulMulKeyPair ( ) {
	}

	public MulMulKeyPair (
			String namedOutput,
			String name,
			int total ) {
		this.namedOutput = namedOutput;
		this.name = name;
		this.total = total;
	}

	@Override
	public void readFields (
			DataInput input ) throws IOException
	{
		this.namedOutput = input.readUTF ( );
		this.name = input.readUTF ( );
		this.total = input.readInt ( );
	}

	@Override
	public void write (
			DataOutput output ) throws IOException
	{
		output.writeUTF ( namedOutput );
		output.writeUTF ( name );
		output.writeInt ( total );
	}

	@Override
	public int compareTo (
			MulMulKeyPair o )
	{
		if ( name.equals ( o.name ) )
		{
			return 0;
		}
		return 1;
	}

	public String getNamedOutput ( )
	{
		return namedOutput;
	}

	public void setNamedOutput (
			String namedOutput )
	{
		this.namedOutput = namedOutput;
	}

	public String getName ( )
	{
		return name;
	}

	public void setName (
			String name )
	{
		this.name = name;
	}

	public int getTotal ( )
	{
		return total;
	}

	public void setTotal (
			int total )
	{
		this.total = total;
	}

	@Override
	public int hashCode ( )
	{
		return name.hashCode ( );
	}

	@Override
	public boolean equals (
			Object obj )
	{
		if ( this == obj )
			return true;
		if ( obj == null )
			return false;
		if ( getClass ( ) != obj.getClass ( ) )
			return false;
		MulMulKeyPair other = ( MulMulKeyPair ) obj;
		if ( name == null )
		{
			if ( other.name != null )
				return false;
		}
		else if ( !name.equals ( other.name ) )
			return false;
		return true;
	}
}
