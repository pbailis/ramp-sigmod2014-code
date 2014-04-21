package edu.berkeley.kaiju.service.request.message.request;

import edu.berkeley.kaiju.data.DataItem;
import edu.berkeley.kaiju.exception.KaijuException;
import edu.berkeley.kaiju.service.LockManager;
import edu.berkeley.kaiju.service.MemoryStorageEngine;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;

import java.util.Collection;
import java.util.List;

public class CheckPreparedRequest extends KaijuMessage implements IKaijuRequest {
    public long timestamp;
    public Collection<String> toCheck;

    public CheckPreparedRequest() { }

    public CheckPreparedRequest(long timestamp, Collection<String> toCheck) {
        this.timestamp = timestamp;
        this.toCheck = toCheck;
    }

    @Override
    public KaijuResponse processRequest(MemoryStorageEngine storageEngine, LockManager lockManager) throws
                                                                                                    KaijuException {
        for(String item : toCheck) {
            if(!storageEngine.isPreparedOrHigherCommitted(item, timestamp)) {
                return new KaijuResponse(false);
            }
        }

        return new KaijuResponse(true);
    }
}