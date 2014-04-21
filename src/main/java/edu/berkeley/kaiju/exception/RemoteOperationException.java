package edu.berkeley.kaiju.exception;

public class RemoteOperationException extends KaijuException {
    public RemoteOperationException(String message) {
        super(message);
    }

    public RemoteOperationException(String message, Throwable throwable) {
        super(message, throwable);
    }
}