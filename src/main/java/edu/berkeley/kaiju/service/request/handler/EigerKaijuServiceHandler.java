package edu.berkeley.kaiju.service.request.handler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.berkeley.kaiju.data.DataItem;
import edu.berkeley.kaiju.exception.HandlerException;
import edu.berkeley.kaiju.net.routing.OutboundRouter;
import edu.berkeley.kaiju.service.request.RequestDispatcher;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.request.EigerGetAllRequest;
import edu.berkeley.kaiju.service.request.message.request.EigerPutAllRequest;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;
import edu.berkeley.kaiju.util.Timestamp;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class EigerKaijuServiceHandler implements IKaijuHandler  {
    RequestDispatcher dispatcher;
    public EigerKaijuServiceHandler(RequestDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    Random random = new Random();

    public Map<String,byte[]> get_all(List<String> keys) throws HandlerException {
        try {
            long readStamp = Timestamp.assignNewTimestamp();

            Map<Integer, Collection<String>> keysByServerID = OutboundRouter.getRouter().groupKeysByServerID(keys);
            Map<Integer, KaijuMessage> requestsByServerID = Maps.newHashMap();

            for(int serverID : keysByServerID.keySet()) {
                requestsByServerID.put(serverID, new EigerGetAllRequest(keysByServerID.get(serverID), readStamp));
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

    @Override
    public void put_all(Map<String, byte[]> keyValuePairs) throws HandlerException {
        try {
            List<String> keys = Lists.newArrayList(keyValuePairs.keySet());
            String coordinatorKey = keys.get(random.nextInt(keys.size()));

            Map<Integer, Collection<String>> keysByServerID = OutboundRouter.getRouter().groupKeysByServerID(
                    keyValuePairs.keySet());
            Map<Integer, KaijuMessage> requestsByServerID = Maps.newHashMap();

            long timestamp = Timestamp.assignNewTimestamp();

            for(int serverID : keysByServerID.keySet()) {
                Map<String, DataItem> keyValuePairsForServer = Maps.newHashMap();
                for(String key : keysByServerID.get(serverID)) {
                    keyValuePairsForServer.put(key, new DataItem(timestamp, keyValuePairs.get(key)));
                }

                requestsByServerID.put(serverID, new EigerPutAllRequest(keyValuePairsForServer,
                                                                        coordinatorKey,
                                                                        keyValuePairs.size()));
            }

            Collection<KaijuResponse> responses = dispatcher.multiRequestBlockFor(requestsByServerID, 1);

            KaijuResponse.coalesceErrorsIntoException(responses);
        } catch (Exception e) {
            throw new HandlerException("Error processing request", e);
        }
    }
}