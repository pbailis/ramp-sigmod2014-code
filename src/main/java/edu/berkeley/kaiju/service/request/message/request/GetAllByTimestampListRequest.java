package edu.berkeley.kaiju.service.request.message.request;

import edu.berkeley.kaiju.exception.KaijuException;
import edu.berkeley.kaiju.service.LockManager;
import edu.berkeley.kaiju.service.MemoryStorageEngine;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;

import java.util.Collection;

public class GetAllByTimestampListRequest extends KaijuMessage implements IKaijuRequest {
    Collection<String> keys;
    Collection<Long> versions;

    private GetAllByTimestampListRequest() {}

    public GetAllByTimestampListRequest(Collection<String> keys, Collection<Long> versions) {
        this.keys = keys;
        this.versions = versions;
    }

    @Override
    public KaijuResponse processRequest(MemoryStorageEngine storageEngine, LockManager lockManager) throws
                                                                                                    KaijuException {
        KaijuResponse ret = new KaijuResponse();
        ret.keyValuePairs = storageEngine.getAllByVersionList(keys, versions);
        return ret;
    }
}