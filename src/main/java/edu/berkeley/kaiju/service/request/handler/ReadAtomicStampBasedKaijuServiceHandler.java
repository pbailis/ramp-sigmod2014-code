package edu.berkeley.kaiju.service.request.handler;

import com.beust.jcommander.internal.Sets;
import com.google.common.collect.Maps;
import edu.berkeley.kaiju.data.DataItem;
import edu.berkeley.kaiju.exception.HandlerException;
import edu.berkeley.kaiju.service.request.RequestDispatcher;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ReadAtomicStampBasedKaijuServiceHandler extends ReadAtomicKaijuServiceHandler {
    public ReadAtomicStampBasedKaijuServiceHandler(RequestDispatcher dispatcher) {
        super(dispatcher);
    }

    public Map<String,byte[]> get_all(List<String> keys) throws HandlerException {
        try {
            Collection<KaijuResponse> first_round_responses = fetch_stamps_from_server(keys);

            Collection<Long> first_round_stamps = Sets.newHashSet();

            for(KaijuResponse response : first_round_responses) {
                first_round_stamps.addAll(response.timestamps);
            }

            Map<String, byte[]> ret = Maps.newHashMap();

            // didn't get any responses
            if(first_round_stamps.isEmpty()) {
                for(String key : keys) {
                    ret.put(key, new byte[0]);
                }

                return ret;
            }

            Collection<KaijuResponse> second_round_responses = fetch_by_stamplist_from_server(keys, first_round_stamps);

            for(KaijuResponse response : second_round_responses) {
                for(Map.Entry<String, DataItem> keyValuePair : response.keyValuePairs.entrySet()) {
                    ret.put(keyValuePair.getKey(), keyValuePair.getValue().getValue());
                }
            }

            return ret;
        } catch (Exception e) {
            throw new HandlerException("Error processing request", e);
        }
    }

    // RAMP-Small just uses a timestamp
    public DataItem instantiateKaijuItem(byte[] value,
                                         Collection<String> allKeys,
                                         long timestamp) {
        return new DataItem(timestamp, value);
    }
}