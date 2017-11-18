
package org.freeknight.framework.helper.net.cause;

public class NotExistIPV4Exception
		extends NetworkException
{

	public NotExistIPV4Exception (
			String message ) {
		super ( message, null );
	}

	public NotExistIPV4Exception (
			String message,
			Throwable cause ) {
		super ( message, cause );
	}

	private static final long	serialVersionUID	= 1L;

}
