package io.github.isuru.oasis.services.exception;

/**
 * @author iweerarathna
 */
public class InputValidationException extends Exception {

    public InputValidationException(String message) {
        super(message);
    }

    public InputValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
