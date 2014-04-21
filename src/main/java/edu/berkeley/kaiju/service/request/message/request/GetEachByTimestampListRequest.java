package edu.berkeley.kaiju.service.request.message.request;

import edu.berkeley.kaiju.exception.KaijuException;
import edu.berkeley.kaiju.service.LockManager;
import edu.berkeley.kaiju.service.MemoryStorageEngine;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;

import java.util.Collection;
import java.util.Map;

public class GetEachByTimestampListRequest extends KaijuMessage implements IKaijuRequest {
    private Map<String, Collection<Long>> keyLists;

    private GetEachByTimestampListRequest() {}

    public GetEachByTimestampListRequest(Map<String, Collection<Long>> keyLists) {
        this.keyLists = keyLists;
    }

    @Override
    public KaijuResponse processRequest(MemoryStorageEngine storageEngine, LockManager lockManager) throws
                                                                                                    KaijuException {
        KaijuResponse ret = new KaijuResponse();
        ret.keyValuePairs = storageEngine.getEachByVersionList(keyLists);
        return ret;
    }
}