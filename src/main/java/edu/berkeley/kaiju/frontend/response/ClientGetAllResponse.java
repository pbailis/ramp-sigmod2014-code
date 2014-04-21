package edu.berkeley.kaiju.frontend.response;

import java.util.Map;

public class ClientGetAllResponse extends ClientResponse {
    public Map<String, byte[]> keyValuePairs;

    ClientGetAllResponse() {}

    public ClientGetAllResponse(Map<String, byte[]> response) {
        this.keyValuePairs = response;
    }
}
