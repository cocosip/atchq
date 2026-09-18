package io.github.cocosip.latchq.exception;

/**
 * Thrown when {@code write}/{@code read}/{@code commit} are called before {@link
 * io.github.cocosip.latchq.LatchQueue#initialize()} or after the queue has been closed.
 */
public class LatchQNotInitializedException extends LatchQException {

    public LatchQNotInitializedException(String message) {
        super(message);
    }
}
