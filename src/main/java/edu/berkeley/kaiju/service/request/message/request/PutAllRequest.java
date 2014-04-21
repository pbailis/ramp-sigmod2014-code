package edu.berkeley.kaiju.service.request.message.request;

import edu.berkeley.kaiju.data.DataItem;
import edu.berkeley.kaiju.exception.KaijuException;
import edu.berkeley.kaiju.service.LockManager;
import edu.berkeley.kaiju.service.MemoryStorageEngine;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;

import java.util.Map;

public class PutAllRequest extends KaijuMessage implements IKaijuRequest {
    public Map<String, DataItem> keyValuePairs;

    private PutAllRequest() {}

    public PutAllRequest(Map<String, DataItem> keyValuePairs) {
        this.keyValuePairs = keyValuePairs;
    }

    @Override
    public KaijuResponse processRequest(MemoryStorageEngine storageEngine, LockManager lockManager) throws
                                                                                                    KaijuException {
        storageEngine.putAll(keyValuePairs);
        return new KaijuResponse();
    }
}