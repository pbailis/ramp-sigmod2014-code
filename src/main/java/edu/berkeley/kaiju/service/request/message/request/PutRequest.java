package edu.berkeley.kaiju.service.request.message.request;

import edu.berkeley.kaiju.data.DataItem;
import edu.berkeley.kaiju.exception.KaijuException;
import edu.berkeley.kaiju.service.LockManager;
import edu.berkeley.kaiju.service.MemoryStorageEngine;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;

public class PutRequest extends KaijuMessage implements IKaijuRequest {
    public String key;
    public DataItem item;

    private PutRequest() {};

    public PutRequest(String key, DataItem item) {
        this.key = key;
        this.item = item;
    }

    @Override
    public KaijuResponse processRequest(MemoryStorageEngine storageEngine, LockManager lockManager) throws
                                                                                                    KaijuException {
        storageEngine.put(key, item);
        return new KaijuResponse();
    }
}