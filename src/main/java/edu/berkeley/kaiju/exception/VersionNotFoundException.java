package edu.berkeley.kaiju.exception;

public class VersionNotFoundException extends KaijuException {
    public VersionNotFoundException(String message) {
        super(message);
    }

    public VersionNotFoundException(String message, Throwable throwable) {
        super(message, throwable);
    }
}