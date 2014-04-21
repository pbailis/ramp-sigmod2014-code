package edu.berkeley.kaiju.frontend.response;

public class ClientError extends ClientResponse {
    public String error;

    ClientError() {}

    public ClientError(String error) {
        this.error = error;
    }

}
