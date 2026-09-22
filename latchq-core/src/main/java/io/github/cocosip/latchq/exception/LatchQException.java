package io.github.cocosip.latchq.exception;

import java.io.Serial;

/** Base runtime exception for all LatchQ errors. Unchecked, following common Java library style. */
public class LatchQException extends RuntimeException {

    @Serial private static final long serialVersionUID = 1L;

    public LatchQException(String message) {
        super(message);
    }

    public LatchQException(String message, Throwable cause) {
        super(message, cause);
    }
}
