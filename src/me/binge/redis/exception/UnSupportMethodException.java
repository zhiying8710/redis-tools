package me.binge.redis.exception;

public class UnSupportMethodException extends Exception {

    private static final long serialVersionUID = 1L;

    public UnSupportMethodException() {
        super();
    }

    public UnSupportMethodException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnSupportMethodException(String message) {
        super(message);
    }

    public UnSupportMethodException(Throwable cause) {
        super(cause);
    }

}
