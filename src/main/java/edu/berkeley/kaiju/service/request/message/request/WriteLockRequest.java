package edu.berkeley.kaiju.service.request.message.request;

import edu.berkeley.kaiju.data.DataItem;
import edu.berkeley.kaiju.exception.KaijuException;
import edu.berkeley.kaiju.service.LockManager;
import edu.berkeley.kaiju.service.LockManager.LockDuration;
import edu.berkeley.kaiju.service.LockManager.LockType;
import edu.berkeley.kaiju.service.MemoryStorageEngine;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;

public class WriteLockRequest extends KaijuMessage implements IKaijuRequest {
    public String key;
    public DataItem item;
    public LockDuration lockDuration;

    private WriteLockRequest() {}

    public WriteLockRequest(String key, DataItem item, LockDuration lockDuration) {
        this.key = key;
        this.item = item;
        this.lockDuration = lockDuration;
    }

    @Override
    public KaijuResponse processRequest(MemoryStorageEngine storageEngine, LockManager lockManager) throws
                                                                                                    KaijuException {
        lockManager.lock(LockType.WRITE, key);
        storageEngine.put(key, item);
        if(lockDuration == LockDuration.SHORT) {
            lockManager.unlock(key);
        }

        return new KaijuResponse();
    }
}