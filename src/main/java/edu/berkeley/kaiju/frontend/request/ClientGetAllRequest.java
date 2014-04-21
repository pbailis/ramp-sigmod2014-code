package edu.berkeley.kaiju.frontend.request;

import java.util.List;

public class ClientGetAllRequest extends ClientRequest {
    public List<String> keys;

    ClientGetAllRequest() {}

    public ClientGetAllRequest(List<String> keys) {
        this.keys = keys;
    }
}
