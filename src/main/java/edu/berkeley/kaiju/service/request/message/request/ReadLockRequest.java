package edu.berkeley.kaiju.service.request.message.request;

import edu.berkeley.kaiju.exception.KaijuException;
import edu.berkeley.kaiju.service.LockManager;
import edu.berkeley.kaiju.service.LockManager.LockDuration;
import edu.berkeley.kaiju.service.LockManager.LockType;
import edu.berkeley.kaiju.service.MemoryStorageEngine;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;

public class ReadLockRequest extends KaijuMessage implements IKaijuRequest {
    public String key;
    public LockDuration lockDuration;

    private ReadLockRequest() {}

    public ReadLockRequest(String key, LockDuration lockDuration) {
        this.key = key;
        this.lockDuration = lockDuration;
    }

    @Override
    public KaijuResponse processRequest(MemoryStorageEngine storageEngine, LockManager lockManager) throws
                                                                                                    KaijuException {
        KaijuResponse ret = new KaijuResponse();
        lockManager.lock(LockType.READ, key);
        ret.dataItem = storageEngine.get(key);
        if(lockDuration == LockDuration.SHORT) {
            lockManager.unlock(key);
        }

        return ret;
    }
}