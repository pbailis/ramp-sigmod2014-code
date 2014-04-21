package edu.berkeley.kaiju.service.request.handler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.berkeley.kaiju.config.Config;
import edu.berkeley.kaiju.data.DataItem;
import edu.berkeley.kaiju.exception.HandlerException;
import edu.berkeley.kaiju.net.routing.OutboundRouter;
import edu.berkeley.kaiju.service.LockManager.LockDuration;
import edu.berkeley.kaiju.service.request.RequestDispatcher;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.request.ReadLockRequest;
import edu.berkeley.kaiju.service.request.message.request.UnlockRequest;
import edu.berkeley.kaiju.service.request.message.request.WriteLockRequest;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;
import edu.berkeley.kaiju.util.Timestamp;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/*
 Handles LWLR, LWNR, NWNR
 */

public class LockBasedKaijuServiceHandler implements IKaijuHandler {
    RequestDispatcher dispatcher;
    ReadCommittedKaijuServiceHandler readCommittedKaijuServiceHandler;
    LockDuration writeDuration = null;
    LockDuration readDuration = null;

    public LockBasedKaijuServiceHandler(RequestDispatcher dispatcher) {
        this.dispatcher = dispatcher;
        readCommittedKaijuServiceHandler = new ReadCommittedKaijuServiceHandler(dispatcher);
        switch (Config.getConfig().isolation_level) {
            case LWLR:
                writeDuration = LockDuration.LONG;
                readDuration = LockDuration.LONG;
                break;
            case LWSR:
                writeDuration = LockDuration.LONG;
                readDuration = LockDuration.SHORT;
                break;
            case LWNR:
                writeDuration = LockDuration.LONG;
                break;
        }
    }

    protected void unlockKeys(Collection<String> keys) throws HandlerException {
        try {
            Map<Integer, Collection<String>> keysByServerID = OutboundRouter.getRouter().groupKeysByServerID(keys);
            Map<Integer, KaijuMessage> requestsByServerID = Maps.newHashMap();

            for(int serverID : keysByServerID.keySet()) {
                requestsByServerID.put(serverID, new UnlockRequest(keysByServerID.get(serverID)));
            }

            Collection<KaijuResponse> responses = dispatcher.multiRequest(requestsByServerID);

            KaijuResponse.coalesceErrorsIntoException(responses);
        } catch (Exception e) {
            throw new HandlerException("Error unlocking keys", e);
        }
    }

    @Override
    public Map<String,byte[]> get_all(List<String> keys) throws HandlerException {
        if(readDuration == null) {
            return readCommittedKaijuServiceHandler.get_all(keys);
        }

        try {
            Collections.sort(keys);

            Map<String, byte[]> ret = Maps.newHashMap();

            for(String key : keys) {
                KaijuResponse response = dispatcher.request(key.hashCode(),
                                                            new ReadLockRequest(key,
                                                                                readDuration));

                if(response.hasErrors()) {
                    throw new HandlerException(response.getErrorString());
                }

                byte[] responseRet = response.dataItem.getValue();

                if(responseRet == null)
                    responseRet = new byte[0];

                ret.put(key, responseRet);
            }

            if(readDuration == LockDuration.LONG) {
                unlockKeys(keys);
            }

            return ret;

        } catch (Exception e) {
            throw new HandlerException("Error processing request", e);
        }
    }

    public void put_all(Map<String, byte[]> keyValuePairs) throws HandlerException {
        if(writeDuration == null) {
            readCommittedKaijuServiceHandler.put_all(keyValuePairs);
            return;
        }

        try {
            List<String> keys = Lists.newArrayList(keyValuePairs.keySet());
            Collections.sort(keys);

            long timestamp = Timestamp.assignNewTimestamp();

            for(String key : keys) {
                KaijuResponse response = dispatcher.request(key.hashCode(),
                                                            new WriteLockRequest(key,
                                                                                 new DataItem(timestamp,
                                                                                              keyValuePairs.get(key)),
                                                                                              writeDuration));

                if(response.hasErrors()) {
                    throw new HandlerException(response.getErrorString());
                }
            }

            unlockKeys(keys);
        } catch (Exception e) {
            throw new HandlerException("Error processing request", e);
        }
    }
}