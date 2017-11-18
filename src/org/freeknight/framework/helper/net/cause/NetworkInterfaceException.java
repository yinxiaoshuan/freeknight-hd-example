
package org.freeknight.framework.helper.net.cause;

/**
 * 网卡异常.
 * 
 * @author yrj
 *
 */
public class NetworkInterfaceException
		extends NetworkException
{
	private static final long	serialVersionUID	= 1L;

	public NetworkInterfaceException (
			String message,
			Throwable cause ) {
		super ( message, cause );
	}

}
