package edu.berkeley.kaiju.service.request.message.request;

import edu.berkeley.kaiju.exception.KaijuException;
import edu.berkeley.kaiju.service.LockManager;
import edu.berkeley.kaiju.service.MemoryStorageEngine;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;

import java.util.Collection;

public class UnlockRequest extends KaijuMessage implements IKaijuRequest {
    public Collection<String> keys;

    private UnlockRequest() {}

    public UnlockRequest(Collection<String> keys) {
        this.keys = keys;
    }

    @Override
    public KaijuResponse processRequest(MemoryStorageEngine storageEngine, LockManager lockManager) throws
                                                                                                    KaijuException {
        lockManager.unlock(keys);
        return new KaijuResponse();
    }
}