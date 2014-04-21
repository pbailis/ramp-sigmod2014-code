package edu.berkeley.kaiju.service.request.message.request;

import com.beust.jcommander.internal.Sets;
import edu.berkeley.kaiju.exception.KaijuException;
import edu.berkeley.kaiju.service.LockManager;
import edu.berkeley.kaiju.service.MemoryStorageEngine;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;

import java.util.Collection;

public class EigerCheckCommitRequest extends KaijuMessage implements IKaijuRequest {
    public Collection<Long> toCheck;
    public long readTimestamp;

    public EigerCheckCommitRequest() {}

    public EigerCheckCommitRequest(long readTimestamp) {
        toCheck = Sets.newHashSet();
        this.readTimestamp = readTimestamp;
    }

    @Override
    public KaijuResponse processRequest(MemoryStorageEngine storageEngine, LockManager lockManager) throws
                                                                                                    KaijuException {
        throw new RuntimeException("SHOULD NOT BE CALLED");
    }
}