package edu.berkeley.kaiju.service.request.message.request;

import edu.berkeley.kaiju.service.LockManager;
import edu.berkeley.kaiju.service.MemoryStorageEngine;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;

public class GetRequest extends KaijuMessage implements IKaijuRequest {
    public String key;

    private GetRequest() {}

    public GetRequest(String key) {
        this.key = key;
    }

    @Override
    public KaijuResponse processRequest(MemoryStorageEngine storageEngine, LockManager lockManager) {
        return new KaijuResponse(storageEngine.get(key));
    }
}