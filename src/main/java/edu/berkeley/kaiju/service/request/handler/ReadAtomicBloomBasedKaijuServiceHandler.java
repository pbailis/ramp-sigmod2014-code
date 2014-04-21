package edu.berkeley.kaiju.service.request.handler;

import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import edu.berkeley.kaiju.config.Config;
import edu.berkeley.kaiju.data.DataItem;
import edu.berkeley.kaiju.exception.HandlerException;
import edu.berkeley.kaiju.service.request.RequestDispatcher;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;
import edu.berkeley.kaiju.util.Timestamp;
import org.apache.hadoop.util.bloom.BloomFilter;

import java.util.*;

public class ReadAtomicBloomBasedKaijuServiceHandler extends ReadAtomicKaijuServiceHandler {
    public ReadAtomicBloomBasedKaijuServiceHandler(RequestDispatcher dispatcher) {
        super(dispatcher);
    }

    public Map<String,byte[]> get_all(List<String> keys) throws HandlerException {
        try {
            Map<String, byte[]> ret = Maps.newHashMap();


            Collection<KaijuResponse> first_round_responses = fetch_from_server(keys);

            List<KeyValuePair> keyValuePairs = new ArrayList<KeyValuePair>();

            for(KaijuResponse response : first_round_responses) {
                for(Map.Entry<String, DataItem> entry : response.keyValuePairs.entrySet()) {
                    keyValuePairs.add(new KeyValuePair(entry.getKey(), entry.getValue()));
                    ret.put(entry.getKey(), entry.getValue().getValue());
                }
            }

            Collections.sort(keyValuePairs);
            DataItem highestItem = keyValuePairs.get(0).value;
            Collections.reverse(keyValuePairs);

            Map<String, Collection<Long>> potentialSiblings = Maps.newHashMap();

            // Algorithm 3, lines 7-10
            for(int i = 0; i < keyValuePairs.size()-1; ++i) {
                KeyValuePair toCheck = keyValuePairs.get(i);

                for(int j = i+1; j < keyValuePairs.size(); ++j) {
                    KeyValuePair toCompare = keyValuePairs.get(j);

                    if(toCompare.value.getTimestamp() == Timestamp.NO_TIMESTAMP ||
                       toCheck.value.getTimestamp() == toCompare.value.getTimestamp()) {
                        continue;
                    }

                    if(toCompare.value.getBloomTransactionKeys().membershipTest(toCheck.key)) {
                        if(!potentialSiblings.containsKey(toCheck.key)) {
                            potentialSiblings.put(toCheck.key, new ArrayList<Long>());
                        }

                        potentialSiblings.get(toCheck.key).add(toCompare.value.getTimestamp());
                    }
                }
            }

            // no missing items and no false positives
            if(potentialSiblings.isEmpty()) {
                return ret;
            }

            Collection<KaijuResponse> second_round_responses = fetch_keys_by_version_lists(potentialSiblings);

            for(KaijuResponse response : second_round_responses) {
                for(Map.Entry<String, DataItem> keyValuePair : response.keyValuePairs.entrySet()) {
                    // note that false positives will return null, so filter them out
                    if(keyValuePair.getValue().getTimestamp() != Timestamp.NO_TIMESTAMP)
                        ret.put(keyValuePair.getKey(), keyValuePair.getValue().getValue());
                }
            }

            return ret;

        } catch (Exception e) {
            throw new HandlerException("Error procesing request", e);
        }
    }

    // utility class for group by
    private class KeyValuePair implements Comparable<KeyValuePair> {
        public String key;
        public DataItem value;

        public KeyValuePair(String key, DataItem value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public int compareTo(KeyValuePair other) {
            return Longs.compare(value.getTimestamp(), other.value.getTimestamp());
        }
    }

    // attach a bloom filter
    public DataItem instantiateKaijuItem(byte[] value,
                                         Collection<String> allKeys,
                                         long timestamp) {
        BloomFilter bloomFilter = new BloomFilter(Config.getConfig().bloom_filter_num_entries,
                                                  Config.getConfig().bloom_filter_hf);
        for(String key : allKeys) {
            bloomFilter.add(key);
        }

        return new DataItem(timestamp, value, bloomFilter);
    }
}