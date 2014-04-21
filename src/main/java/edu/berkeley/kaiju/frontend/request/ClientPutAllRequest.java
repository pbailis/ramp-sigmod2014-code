package edu.berkeley.kaiju.frontend.request;

import java.util.Map;

public class ClientPutAllRequest extends ClientRequest {
    public Map<String, byte[]> keyValuePairs;

    ClientPutAllRequest() {}

    public ClientPutAllRequest(Map<String, byte[]> keyValuePairs) {
        this.keyValuePairs = keyValuePairs;
    }
}
