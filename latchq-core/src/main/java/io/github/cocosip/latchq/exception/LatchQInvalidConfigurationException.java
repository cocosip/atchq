package io.github.cocosip.latchq.exception;

import java.io.Serial;

/**
 * Thrown when required configuration is missing or invalid, for example a blank {@code rootPath}, a
 * blank {@code fileName}, an unknown queue name, or an unknown roll cycle name.
 */
public class LatchQInvalidConfigurationException extends LatchQException {

    @Serial private static final long serialVersionUID = 1L;

    public LatchQInvalidConfigurationException(String message) {
        super(message);
    }

    public LatchQInvalidConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
