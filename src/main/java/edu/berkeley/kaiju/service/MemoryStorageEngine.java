package edu.berkeley.kaiju.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Queues;
import com.yammer.metrics.Gauge;
import com.yammer.metrics.Meter;
import com.yammer.metrics.MetricRegistry;
import edu.berkeley.kaiju.config.Config;
import edu.berkeley.kaiju.config.Config.IsolationLevel;
import edu.berkeley.kaiju.data.DataItem;
import edu.berkeley.kaiju.data.ItemVersion;
import edu.berkeley.kaiju.exception.AbortedException;
import edu.berkeley.kaiju.exception.KaijuException;
import edu.berkeley.kaiju.monitor.MetricsManager;
import edu.berkeley.kaiju.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/*
 Fairly simple in-memory KVS with limited support for multiversioning.

 The annoying bit comes in implementing so many different algorithms, each of which requires accesses to
 various indexes.
 */
public class MemoryStorageEngine {

    private Meter overwrittenMeter = MetricsManager.getRegistry().meter(MetricRegistry.name(MemoryStorageEngine.class,
                                                                                            "put-requests",
                                                                                            "overwrites"));

    private Meter nopWriteMeter = MetricsManager.getRegistry().meter(MetricRegistry.name(MemoryStorageEngine.class,
                                                                                         "put-requests",
                                                                                         "no-overwrites"));

    private Meter gcWriteMeter = MetricsManager.getRegistry().meter(MetricRegistry.name(MemoryStorageEngine.class,
                                                                                        "gc-request",
                                                                                        "gc-events"));

    private Gauge<Integer> gcQueueSize = MetricsManager.getRegistry().register(MetricRegistry.name(MemoryStorageEngine.class,
                                                                                                   "gc-queue",
                                                                                                   "size"),
                                                                               new Gauge<Integer>() {
                                                                                   @Override
                                                                                   public Integer getValue() {
                                                                                       return candidatesForGarbageCollection

                                                                                               .size();
                                                                                   }
                                                                               });

    private Gauge<Integer> numVersions = MetricsManager.getRegistry().register(MetricRegistry.name(MemoryStorageEngine.class,
                                                                                                   "datastore",
                                                                                                   "version-count"),
                                                                                   new Gauge<Integer>() {
                                                                                       @Override
                                                                                       public Integer getValue() {
                                                                                           return dataItems.size();
                                                                                       }
                                                                                   });

    private Gauge<Integer> numKeys = MetricsManager.getRegistry().register(MetricRegistry.name(MemoryStorageEngine.class,
                                                                                                   "datastore",
                                                                                                   "key-count"),
                                                                                   new Gauge<Integer>() {
                                                                                       @Override
                                                                                       public Integer getValue() {
                                                                                           return lastCommitForKey.size();
                                                                                       }
                                                                                   });

    private static final long gcTimeMs = Config.getConfig().overwrite_gc_ms;

    private static Logger logger = LoggerFactory.getLogger(MemoryStorageEngine.class);

    /*
     We basically implement the KVS as a map from [Key, Timestamp] -> value, with an index on
     Key -> Last Committed Timestamp.

     We experimented with NonBlockingHashMap, but didn't have much luck.
     */

    // 'versions' in the pseudocode; using KeyTimestampPair helper makes GC easier than nesting maps
    private ConcurrentMap<KeyTimestampPair, DataItem> dataItems = Maps.newConcurrentMap();

    // 'lastCommit' in the pseudocode
    private ConcurrentMap<String, Long> lastCommitForKey = Maps.newConcurrentMap();

    // when we get a 'commit' message, this map tells us which [Key, Timestamp] pairs were actually committed
    private ConcurrentMap<Long, List<KeyTimestampPair>> preparedNotCommittedByStamp = Maps.newConcurrentMap();

    // only used in E-PCI, which requires ordering for lookups
    private ConcurrentMap<String, ConcurrentSkipListMap<Long, DataItem>> eigerMap = Maps.newConcurrentMap();

    // used in CTP
    private ConcurrentMap<Long, Boolean> abortedTxns = Maps.newConcurrentMap();

    private boolean isEiger = Config.getConfig().isolation_level == IsolationLevel.EIGER;

    // a roughly time-ordered queue of KVPs to GC; exact real-time ordering not necessary for correctness
    private BlockingQueue<KeyTimestampPair> candidatesForGarbageCollection = Queues.newLinkedBlockingQueue();

    public MemoryStorageEngine() {
        // GC old versions
        new Thread(new Runnable() {
            @Override
            public void run() {
                long currentTime = -1;
                KeyTimestampPair nextStamp = null;
                while(true) {
                    try {
                        if(nextStamp == null)
                            nextStamp = candidatesForGarbageCollection.take();
                        if(nextStamp.getExpirationTime() < currentTime ||
                           (nextStamp.getExpirationTime() < (currentTime = System.currentTimeMillis())) ) {
                            dataItems.remove(nextStamp);

                            if(isEiger)
                                eigerMap.get(nextStamp.getKey()).remove(nextStamp.getTimestamp());
                            gcWriteMeter.mark();
                            nextStamp = null;
                        } else {
                            Thread.sleep(nextStamp.getExpirationTime()-currentTime);
                        }
                    } catch (InterruptedException e) {}
                }
            }
        }, "Storage-GC-Thread").start();
    }

    // get last committed write for each key
    public Map<String, DataItem> getAll(Collection<String> keys) throws KaijuException {
        HashMap<String, DataItem> results = Maps.newHashMap();

        for(String key : keys) {
            DataItem item = getLatestItemForKey(key);

            if(item == null)
                item = DataItem.getNullItem();

            results.put(key, item);
        }

        return results;
    }

    // get last commited timestamp for set of keys
    public Collection<Long> getTimestamps(Collection<String> keys) throws KaijuException {
        Collection<Long> results = Lists.newArrayList();

        for(String key : keys) {
            if(lastCommitForKey.containsKey(key))
                results.add(lastCommitForKey.get(key));
        }

        return results;
    }

    // probably could have passed a map, in retrospect
    public Map<String, DataItem> getAllByVersion(Collection<ItemVersion> versions) throws KaijuException {
        HashMap<String, DataItem> results = Maps.newHashMap();

        for(ItemVersion version : versions) {
            results.put(version.getKey(), getByTimestamp(version.getKey(), version.getTimestamp()));
        }

        return results;
    }

    // find the highest timestamped version of each key in keys that is present in versions (RAMP-Small)
    public Map<String, DataItem> getAllByVersionList(Collection<String> keys,
                                                     Collection<Long> versions) throws KaijuException {
        HashMap<String, DataItem> results = Maps.newHashMap();

        List<Long> timestampList = Ordering.natural().reverse().sortedCopy(versions);

        for(String key : keys) {
            DataItem item = getByTimestampList(key, timestampList);

            if(item == null)
                item = DataItem.getNullItem();

            results.put(key, item);
        }

        return results;
    }

    // used in RAMP-Hybrid; allow false positives
    public Map<String, DataItem> getEachByVersionList(Map<String, Collection<Long>> keyVersions) throws KaijuException {
        HashMap<String, DataItem> results = Maps.newHashMap();

        for(String key : keyVersions.keySet()) {
            Collection<Long> versions = keyVersions.get(key);
            List<Long> timestampList = Ordering.natural().reverse().sortedCopy(versions);

            DataItem item = getByTimestampList(key, timestampList);

            if(item == null)
                item = DataItem.getNullItem();

            results.put(key, item);
        }

        return results;
    }

    public DataItem get(String key) {
        return getLatestItemForKey(key);
    }

    private DataItem getByTimestamp(String key, Long requiredTimestamp) throws KaijuException {
        assert(requiredTimestamp != Timestamp.NO_TIMESTAMP);

        DataItem ret = getItemByVersion(key, requiredTimestamp);

        if(ret == null)
            logger.warn("No suitable value found for key " + key
                                               + " version " + requiredTimestamp);

        return ret;
    }

    // return the highest found timestamp that matches the list
    // assumes that inputTimestampList is sorted
    private DataItem getByTimestampList(String key, List<Long> inputTimestampList) throws KaijuException {
        // have to examine pending items now; look from highest to lowest
        for(long candidateStamp : inputTimestampList) {
            DataItem candidate = getItemByVersion(key, candidateStamp);
            if(candidate != null)
                return candidate;
        }

        return null;
    }

    private DataItem getLatestItemForKey(String key) {
        if(!lastCommitForKey.containsKey(key))
            return DataItem.getNullItem();

        return getItemByVersion(key, lastCommitForKey.get(key));
    }

    private DataItem getItemByVersion(String key, long timestamp) {
        return dataItems.get(new KeyTimestampPair(key,  timestamp));
    }

    public void putAll(Map<String, DataItem> pairs) throws KaijuException {
        assert(!pairs.isEmpty());

        for(Map.Entry<String, DataItem> pair : pairs.entrySet()) {
            put(pair.getKey(), pair.getValue());
        }
    }

    public void put(String key, DataItem value) throws KaijuException {
        prepare(key, value);
        commit(key, value.getTimestamp());
    }

    private void commit(String key, Long timestamp) throws KaijuException {
        // put if newer
        while(true) {
            Long oldCommitted = lastCommitForKey.get(key);

            if(oldCommitted == null) {
                if(lastCommitForKey.putIfAbsent(key, timestamp) == null) {
                    break;
                }
            } else if(oldCommitted < timestamp) {
                if(lastCommitForKey.replace(key, oldCommitted, timestamp)) {
                    markForGC(key, oldCommitted);
                    overwrittenMeter.mark();
                    break;
                }
            } else {
                markForGC(key, timestamp);
                nopWriteMeter.mark();
                break;
            }
        }
    }

    public void prepare(Map<String, DataItem> pairs) throws KaijuException {
        if(pairs.isEmpty()) {
            logger.warn("prepare of zero key value pairs?");
            return;
        }

        // all pairs will have the same timestamp, but we still send the
        // pairs with separate timestamps because they'll be stored that way
        long timestamp = pairs.values().iterator().next().getTimestamp();

        if(abortedTxns.containsKey(timestamp)) {
            throw new AbortedException("Timestamp was already aborted pre-commit "+timestamp);
        }

        List<KeyTimestampPair> pendingPairs = Lists.newArrayList();

        for(Map.Entry<String, DataItem> pair : pairs.entrySet()) {
            prepare(pair.getKey(), pair.getValue());
            pendingPairs.add(new KeyTimestampPair(pair.getKey(), pair.getValue().getTimestamp()));
        }

        preparedNotCommittedByStamp.put(timestamp, pendingPairs);
    }

    public void commit(long timestamp) throws KaijuException {
        List<KeyTimestampPair> toUpdate = preparedNotCommittedByStamp.get(timestamp);

        if(toUpdate == null) {
            return;
        }

        for(KeyTimestampPair pair : toUpdate) {
            commit(pair.getKey(), pair.getTimestamp());
        }

        preparedNotCommittedByStamp.remove(timestamp);
    }

    private void prepare(String key, DataItem value) {
        dataItems.put(new KeyTimestampPair(key, value.getTimestamp()), value);

        if(isEiger) {
            if(!eigerMap.containsKey(key))
                eigerMap.putIfAbsent(key, new ConcurrentSkipListMap<Long, DataItem>());

            eigerMap.get(key).put(value.getTimestamp(), value);
        }
    }

    public DataItem getHighestNotGreaterThan(String key, long timestamp) {
        assert(isEiger);

        ConcurrentSkipListMap<Long, DataItem> skipListMap = eigerMap.get(key);
        if(skipListMap == null)
            return DataItem.getNullItem();

        Map.Entry<Long, DataItem> ret = skipListMap.floorEntry(timestamp);
        if(ret == null)
            return DataItem.getNullItem();

        return ret.getValue();
    }

    public boolean isPreparedOrHigherCommitted(String item, long timestamp) {
        if(abortedTxns.containsKey(timestamp)) {
            return false;
        }

        Long latestItem = lastCommitForKey.get(item);
        if(latestItem != null && latestItem >= timestamp) {
            return true;
        }

        if(preparedNotCommittedByStamp.containsKey(timestamp)) {
            return true;
        }

        latestItem = lastCommitForKey.get(item);
        if(latestItem != null && latestItem >= timestamp) {
            return true;
        }

        abortedTxns.put(timestamp, Boolean.TRUE);
        return false;
    }

    private void markForGC(String key, long timestamp) {
        KeyTimestampPair stamp = new KeyTimestampPair(key,
                                                      timestamp,
                                                      System.currentTimeMillis()+gcTimeMs);
        candidatesForGarbageCollection.add(stamp);
    }

    private class KeyTimestampPair {
        private String key;
        private long timestamp;
        private long expirationTime = -1;

        public KeyTimestampPair(String key, long timestamp) {
            this.key = key;
            this.timestamp = timestamp;
        }

        public KeyTimestampPair(String key, long timestamp, long expirationTime) {
            this(key, timestamp);
            this.expirationTime = expirationTime;
        }

        public long getExpirationTime(){
            return expirationTime;
        }

        private String getKey() {
            return key;
        }

        private long getTimestamp() {
            return timestamp;
        }

        @Override
        public int hashCode() {
            return key.hashCode()*Long.valueOf(timestamp).hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null)
                 return false;
             if (obj == this)
                 return true;
             if (!(obj instanceof KeyTimestampPair))
                 return false;

             KeyTimestampPair rhs = (KeyTimestampPair) obj;
             return rhs.getTimestamp() == timestamp && rhs.getKey().equals(key);
        }
    }

    public Iterable<Long> getPendingStamps() {
        return preparedNotCommittedByStamp.keySet();
    }

    public Collection<String> getPendingKeys(long timestamp) throws KaijuException {
        List<KeyTimestampPair> pairs = preparedNotCommittedByStamp.get(timestamp);

        if(pairs == null || pairs.isEmpty()) {
            return null;
        }

        KeyTimestampPair pair = pairs.iterator().next();
        return getByTimestamp(pair.key, pair.timestamp).getTransactionKeys();
    }

    // only used in CTP
    public void abort(long timestamp) {
        abortedTxns.put(timestamp, Boolean.TRUE);
        List<KeyTimestampPair> pairs = preparedNotCommittedByStamp.remove(timestamp);

        if(pairs != null) {
            for(KeyTimestampPair pair : pairs) {
                dataItems.remove(pair);
            }
        }
    }

    public void reset() {
        preparedNotCommittedByStamp.clear();
        dataItems.clear();
        eigerMap.clear();
        lastCommitForKey.clear();
        candidatesForGarbageCollection.clear();
        isEiger = Config.getConfig().isolation_level == IsolationLevel.EIGER;
    }
}