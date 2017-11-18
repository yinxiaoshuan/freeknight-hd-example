
package org.freeknight.framework.helper.net.cause;

/**
 * 网络异常基类.
 * 
 * @author yrj
 *
 */
public class NetworkException
		extends RuntimeException
{
	private static final long	serialVersionUID	= 1L;

	public NetworkException (
			String message,
			Throwable cause ) {
		super ( message, cause );
	}
}
