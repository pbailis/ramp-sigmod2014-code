package edu.berkeley.kaiju.service.request.message.request;

import edu.berkeley.kaiju.exception.KaijuException;
import edu.berkeley.kaiju.service.LockManager;
import edu.berkeley.kaiju.service.MemoryStorageEngine;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;

import java.util.Collection;

public class EigerGetAllRequest extends KaijuMessage implements IKaijuRequest {
    public Collection<String> keys;
    public long readTimestamp;

    private EigerGetAllRequest() {}

    public EigerGetAllRequest(Collection<String> keys, long readTimestamp) {
        this.keys = keys;
        this.readTimestamp = readTimestamp;
    }

    @Override
    public KaijuResponse processRequest(MemoryStorageEngine storageEngine, LockManager lockManager) throws
                                                                                                    KaijuException {
        throw new RuntimeException("SHOULD NOT BE CALLED");
    }
}