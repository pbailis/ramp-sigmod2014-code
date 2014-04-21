package edu.berkeley.kaiju.service.request.handler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.berkeley.kaiju.data.DataItem;
import edu.berkeley.kaiju.exception.HandlerException;
import edu.berkeley.kaiju.service.request.RequestDispatcher;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;
import edu.berkeley.kaiju.util.Timestamp;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ReadAtomicListBasedKaijuServiceHandler extends ReadAtomicKaijuServiceHandler {

    public ReadAtomicListBasedKaijuServiceHandler(RequestDispatcher dispatcher) {
        super(dispatcher);
    }

    public Map<String,byte[]> get_all(List<String> keys) throws HandlerException {
        try {
            Collection<KaijuResponse> first_round_responses = fetch_from_server(keys);

            Map<String, Long> latestUpdateForKey = Maps.newHashMap();

            // latestUpdateForKey will contain v_latest in Algorithm 1
            for(KaijuResponse response : first_round_responses) {
                for(Map.Entry<String, DataItem> keyValuePair : response.keyValuePairs.entrySet()) {
                    if(keyValuePair.getValue().getTransactionKeys() == null ||
                       keyValuePair.getValue().getTransactionKeys().isEmpty()) {

                        if(!latestUpdateForKey.containsKey(keyValuePair.getKey())) {
                            latestUpdateForKey.put(keyValuePair.getKey(), Timestamp.NO_TIMESTAMP);
                        }
                        continue;
                    }

                    for(String key : keyValuePair.getValue().getTransactionKeys()) {
                        if(!latestUpdateForKey.containsKey(key) || latestUpdateForKey.get(key) < keyValuePair.getValue().getTimestamp()) {
                            latestUpdateForKey.put(key, keyValuePair.getValue().getTimestamp());
                        }
                    }


                }
            }

            Map<String, byte[]> ret = Maps.newHashMap();

            Map<String, Long> second_round_required = Maps.newHashMap();

            for(KaijuResponse response : first_round_responses) {
                for(Map.Entry<String, DataItem> keyValuePair : response.keyValuePairs.entrySet()) {
                    long latestSeenValue = latestUpdateForKey.get(keyValuePair.getKey());
                    // if we need to fetch an updated value
                    if(latestSeenValue > keyValuePair.getValue().getTimestamp()) {
                        second_round_required.put(keyValuePair.getKey(), latestSeenValue);
                    }
                    // saw the last updated value
                    else {
                        ret.put(keyValuePair.getKey(), keyValuePair.getValue().getValue());
                    }
                }
            }

            // weren't missing any updates!
            if(second_round_required.isEmpty())
                return ret;

            Collection<KaijuResponse> second_round_responses = fetch_by_version_from_server(second_round_required);

            for(KaijuResponse response : second_round_responses) {
                for(Map.Entry<String, DataItem> keyValuePair : response.keyValuePairs.entrySet()) {
                    ret.put(keyValuePair.getKey(),  keyValuePair.getValue().getValue());
                }
            }

            return ret;
        } catch (Exception e) {
            throw new HandlerException("Error processing request", e);
        }
    }

    // attach the write set as metadata
    public DataItem instantiateKaijuItem(byte[] value,
                                         Collection<String> allKeys,
                                         long timestamp) {
        return new DataItem(timestamp, value, Lists.newArrayList(allKeys));
    }
}