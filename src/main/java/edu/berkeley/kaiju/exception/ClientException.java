package edu.berkeley.kaiju.exception;

public class ClientException extends KaijuException {
    public ClientException(String message) {
        super(message);
    }

    public ClientException(String message, Throwable throwable) {
        super(message, throwable);
    }
}