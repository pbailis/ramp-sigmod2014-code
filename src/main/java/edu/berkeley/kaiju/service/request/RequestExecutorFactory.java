package edu.berkeley.kaiju.service.request;

import edu.berkeley.kaiju.service.LockManager;
import edu.berkeley.kaiju.service.MemoryStorageEngine;
import edu.berkeley.kaiju.service.request.eiger.EigerExecutor;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;

/*
 Convenience wrapper for passing the right params to
 */
public class RequestExecutorFactory {
    private MemoryStorageEngine storageEngine;
    private LockManager lockManager;
    private EigerExecutor eigerExecutor;

    public RequestExecutorFactory(MemoryStorageEngine storageEngine, LockManager lockManager) {
        this.storageEngine = storageEngine;
        this.lockManager = lockManager;
    }

    /*
     The 2PC-CI code in general is a bit annoying/was added later on in the dev cycle.
     Artifacts like this are lamentable.
     */
    public void setEigerExecutor(EigerExecutor eigerExecutor) {
        this.eigerExecutor = eigerExecutor;
    }

    public RequestExecutor createExecutor(RequestDispatcher responseDispatcher,
                                          KaijuMessage message) {
        return new RequestExecutor(responseDispatcher, storageEngine, lockManager, message, eigerExecutor);
    }
}