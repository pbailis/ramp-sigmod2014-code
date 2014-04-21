package edu.berkeley.kaiju.service.request.handler;

import com.google.common.collect.Maps;
import edu.berkeley.kaiju.data.DataItem;
import edu.berkeley.kaiju.exception.HandlerException;
import edu.berkeley.kaiju.net.routing.OutboundRouter;
import edu.berkeley.kaiju.service.request.RequestDispatcher;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.request.GetAllRequest;
import edu.berkeley.kaiju.service.request.message.request.PutAllRequest;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;
import edu.berkeley.kaiju.util.Timestamp;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/*
 Vanilla NWNR implementation.
 */
public class ReadCommittedKaijuServiceHandler implements IKaijuHandler {
    RequestDispatcher dispatcher;
    public ReadCommittedKaijuServiceHandler(RequestDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public Map<String,byte[]> get_all(List<String> keys) throws HandlerException {
        try {
            Map<Integer, Collection<String>> keysByServerID = OutboundRouter.getRouter().groupKeysByServerID(keys);
            Map<Integer, KaijuMessage> requestsByServerID = Maps.newHashMap();

            for(int serverID : keysByServerID.keySet()) {
                requestsByServerID.put(serverID, new GetAllRequest(keysByServerID.get(serverID)));
            }

            Collection<KaijuResponse> responses = dispatcher.multiRequest(requestsByServerID);

            Map<String, byte[]> ret = Maps.newHashMap();

            KaijuResponse.coalesceErrorsIntoException(responses);

            for(KaijuResponse response : responses) {
                for(Map.Entry<String, DataItem> keyValuePair : response.keyValuePairs.entrySet()) {
                    ret.put(keyValuePair.getKey(), keyValuePair.getValue().getValue());
                }
            }

            return ret;
        } catch (Exception e) {
            throw new HandlerException("Error processing request", e);
        }
    }

    public void put_all(Map<String, byte[]> keyValuePairs) throws HandlerException {
        try {
            Map<Integer, Collection<String>> keysByServerID = OutboundRouter.getRouter().groupKeysByServerID(keyValuePairs.keySet());
            Map<Integer, KaijuMessage> requestsByServerID = Maps.newHashMap();

            long timestamp = Timestamp.assignNewTimestamp();

            for(int serverID : keysByServerID.keySet()) {
                Map<String, DataItem> keyValuePairsForServer = Maps.newHashMap();
                for(String key : keysByServerID.get(serverID)) {
                    keyValuePairsForServer.put(key, new DataItem(timestamp, keyValuePairs.get(key)));
                }

                requestsByServerID.put(serverID, new PutAllRequest(keyValuePairsForServer));
            }

            Collection<KaijuResponse> responses = dispatcher.multiRequest(requestsByServerID);

            KaijuResponse.coalesceErrorsIntoException(responses);
        } catch (Exception e) {
            throw new HandlerException("Error processing request", e);
        }
    }
}