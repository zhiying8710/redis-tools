package me.binge.redis.exception;

public class RedisExecExecption extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public RedisExecExecption(String msg, Exception e) {
		super(msg, e);
	}

	public RedisExecExecption() {
		super();
	}

	public RedisExecExecption(String message, Throwable cause) {
		super(message, cause);
	}

	public RedisExecExecption(String message) {
		super(message);
	}

	public RedisExecExecption(Throwable cause) {
		super(cause);
	}


}
