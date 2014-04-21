package edu.berkeley.kaiju.service.request.message.request;

import edu.berkeley.kaiju.data.ItemVersion;
import edu.berkeley.kaiju.exception.KaijuException;
import edu.berkeley.kaiju.service.LockManager;
import edu.berkeley.kaiju.service.MemoryStorageEngine;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;

import java.util.Collection;

public class GetAllByTimestampRequest extends KaijuMessage implements IKaijuRequest {
    public Collection<ItemVersion> versions;

    private GetAllByTimestampRequest() {}

    public GetAllByTimestampRequest(Collection<ItemVersion> versions) {
        this.versions = versions;
    }

    @Override
    public KaijuResponse processRequest(MemoryStorageEngine storageEngine, LockManager lockManager) throws
                                                                                                    KaijuException {
        KaijuResponse ret = new KaijuResponse();
        ret.keyValuePairs = storageEngine.getAllByVersion(versions);
        return ret;
    }
}