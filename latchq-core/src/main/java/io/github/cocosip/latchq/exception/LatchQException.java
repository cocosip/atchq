package io.github.cocosip.latchq.exception;

/** Base runtime exception for all LatchQ errors. Unchecked, following common Java library style. */
public class LatchQException extends RuntimeException {

    public LatchQException(String message) {
        super(message);
    }

    public LatchQException(String message, Throwable cause) {
        super(message, cause);
    }
}
