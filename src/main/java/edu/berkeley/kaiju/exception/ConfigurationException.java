package edu.berkeley.kaiju.exception;

public class ConfigurationException extends KaijuException {
    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Throwable throwable) {
        super(message, throwable);
    }
}