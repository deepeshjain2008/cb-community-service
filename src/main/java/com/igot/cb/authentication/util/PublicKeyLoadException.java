package com.igot.cb.authentication.util;

/**
 * Thrown when a public key cannot be loaded from its string representation.
 */
public class PublicKeyLoadException extends Exception {

    public PublicKeyLoadException(String message, Throwable cause) {
        super(message, cause);
    }
}
