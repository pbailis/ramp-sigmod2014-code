package edu.berkeley.kaiju.service.request.message.request;

import edu.berkeley.kaiju.exception.KaijuException;
import edu.berkeley.kaiju.service.LockManager;
import edu.berkeley.kaiju.service.MemoryStorageEngine;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;

/*
 Every RPC (except for Eiger/E-PCI requests) simply extends the following interface, which is executed on the
 remote server (and passed a handle to its storage engine and lock manager.)
 */
public interface IKaijuRequest {
    public KaijuResponse processRequest(MemoryStorageEngine engine, LockManager lockManager) throws KaijuException;
}