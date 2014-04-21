package edu.berkeley.kaiju.exception;

public class AbortedException extends KaijuException {
    public AbortedException(String message) {
        super(message);
    }

    public AbortedException(String message, Throwable throwable) {
        super(message, throwable);
    }
}