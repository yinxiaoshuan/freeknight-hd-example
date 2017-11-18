
package org.freeknight.framework.helper.net;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import org.freeknight.framework.helper.net.cause.NetworkInterfaceException;
import org.freeknight.framework.helper.net.cause.NotExistIPV4Exception;

public final class NetworkUtils
{

	private final static String	LOCALHOST	= "127.0.0.1";

	private final static String	ANYHOST		= "0.0.0.0";

	/**
	 * 获取本地可用IP地址.
	 * 
	 * @return
	 */
	public static final NetworkAddress getAvailableNetAddress ( )
	{
		InetAddress address = null;
		Enumeration< NetworkInterface > interfaces = getAllNetworkInterfaces ( );
		for ( ; interfaces.hasMoreElements ( ); )
		{
			NetworkInterface networkInterface = interfaces.nextElement ( );
			boolean available = available ( networkInterface );
			if ( !available )
			{
				continue;
			}

			boolean flag = false;
			Enumeration< InetAddress > addresses = networkInterface.getInetAddresses ( );
			for ( ; addresses.hasMoreElements ( ); )
			{
				InetAddress addr = addresses.nextElement ( );
				if ( isValidAndIPV4 ( addr ) )
				{
					address = addr;
					flag = true;
					break;
				}
			}

			if ( flag && address != null )
			{
				String macAddress = getMAC ( address );
				NetworkAddress netAddr = new NetworkAddress ( networkInterface.getName ( ), address.getHostAddress ( ), macAddress );
				return netAddr;
			}

		}

		throw new NotExistIPV4Exception ( "IPV4 no exists." );
	}

	private static String getMAC (
			InetAddress address )
	{
		try
		{
			byte[ ] macBuf = NetworkInterface.getByInetAddress ( address ).getHardwareAddress ( );
			return toHexHardwareAddress ( macBuf );
		}
		catch ( SocketException e )
		{
			throw new NetworkInterfaceException ( "Mac Address failture.", e );
		}
	}

	/**
	 * 将MAC地址转换为可识别的字符串.
	 * 
	 * @param hardware
	 * @return
	 */
	private static String toHexHardwareAddress (
			final byte[ ] hardware )
	{
		final StringBuilder mac = new StringBuilder ( );
		for ( int i = 0; i < hardware.length; i++ )
		{
			if ( i != 0 )
			{
				mac.append ( ":" );
			}

			final String hex = Integer.toHexString ( hardware [ i ] & 0xf );
			mac.append ( hex.length ( ) == 1 ? ( 0 + hex ) : hex );
		}
		return mac.toString ( );
	}

	private static boolean isValidAndIPV4 (
			InetAddress address )
	{
		if ( address.isLoopbackAddress ( ) )
		{
			return false;
		}

		final String name = address.getHostAddress ( );
		if ( name.length ( ) > 16 )// 只取IPV4
		{
			return false;
		}

		return ( !ANYHOST.equals ( name ) && !LOCALHOST.equals ( name ) );
	}

	/*
	 * 网卡是否可用.
	 */
	private static boolean available (
			NetworkInterface networkInterface )
	{
		try
		{
			if ( !networkInterface.isUp ( ) /* 是否已开启并运行 */
					|| networkInterface.isLoopback ( ) /* 回送接口 */
					|| networkInterface.isVirtual ( ) /* 虚拟接口, 类似 VMware. */)
			{
				return false;
			}
		}
		catch ( SocketException ignore )
		{
			// 读取该网卡错误, 默认丢弃掉.
			return false;
		}
		return true;
	}

	/**
	 * 获取本机所有网卡.
	 * 
	 * @return
	 */
	private static Enumeration< NetworkInterface > getAllNetworkInterfaces ( )
	{
		try
		{
			return NetworkInterface.getNetworkInterfaces ( );
		}
		catch ( SocketException e )
		{
			throw new NetworkInterfaceException ( "读取网卡失败", e );
		}
	}

	private NetworkUtils ( ) {
	}
}
