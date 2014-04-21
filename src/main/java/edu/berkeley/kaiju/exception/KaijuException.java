package edu.berkeley.kaiju.exception;

public abstract class KaijuException extends Exception
{
    public KaijuException(String message) {
        super(message);
    }

    public KaijuException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
