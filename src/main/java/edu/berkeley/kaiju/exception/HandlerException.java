package edu.berkeley.kaiju.exception;

public class HandlerException extends KaijuException {
    public HandlerException(String message) {
        super(message);
    }

    public HandlerException(String message, Throwable throwable) {
        super(message, throwable);
    }
}