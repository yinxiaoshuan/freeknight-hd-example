
package org.freeknight.framework.helper.net;

import java.io.Serializable;

/**
 * 网络地址信息.
 * 
 * @author yrj
 *
 */
public class NetworkAddress
		implements Serializable
{
	private static final long	serialVersionUID	= 1L;

	/* 网卡 */
	private String						interfaceName;

	/* IP地址 */
	private String						hostName;

	/* 本机MAC地址 */
	private String						macName;

	public NetworkAddress ( ) {
	}

	public NetworkAddress (
			String interfaceName,
			String hostName,
			String macName ) {
		this.interfaceName = interfaceName;
		this.hostName = hostName;
		this.macName = macName;
	}

	public String getInterfaceName ( )
	{
		return interfaceName;
	}

	public void setInterfaceName (
			String interfaceName )
	{
		this.interfaceName = interfaceName;
	}

	public String getHostName ( )
	{
		return hostName;
	}

	public void setHostName (
			String hostName )
	{
		this.hostName = hostName;
	}

	public String getMacName ( )
	{
		return macName;
	}

	public void setMacName (
			String macName )
	{
		this.macName = macName;
	}

	@Override
	public String toString ( )
	{
		StringBuilder tostr = new StringBuilder ( );
		tostr.append ( "NetworkAddress[" );
		tostr.append ( "interface=" ).append ( interfaceName );
		tostr.append ( ", host=" ).append ( hostName );
		tostr.append ( ", mac=" ).append ( macName );
		tostr.append ( "]" );
		return tostr.toString ( );
	}
}
