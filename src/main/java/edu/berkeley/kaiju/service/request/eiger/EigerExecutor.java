package edu.berkeley.kaiju.service.request.eiger;

import com.beust.jcommander.internal.Sets;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.yammer.metrics.Histogram;
import com.yammer.metrics.MetricRegistry;
import com.yammer.metrics.Timer;
import com.yammer.metrics.Timer.Context;
import edu.berkeley.kaiju.config.Config;
import edu.berkeley.kaiju.data.DataItem;
import edu.berkeley.kaiju.exception.KaijuException;
import edu.berkeley.kaiju.monitor.MetricsManager;
import edu.berkeley.kaiju.net.routing.OutboundRouter;
import edu.berkeley.kaiju.service.MemoryStorageEngine;
import edu.berkeley.kaiju.service.request.RequestDispatcher;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.request.EigerCheckCommitRequest;
import edu.berkeley.kaiju.service.request.message.request.EigerCommitRequest;
import edu.berkeley.kaiju.service.request.message.request.EigerGetAllRequest;
import edu.berkeley.kaiju.service.request.message.request.EigerPutAllRequest;
import edu.berkeley.kaiju.service.request.message.response.EigerCheckCommitResponse;
import edu.berkeley.kaiju.service.request.message.response.EigerPreparedResponse;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;
import edu.berkeley.kaiju.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/*
  The E-PCI algorithm was pretty complicated since it required remote RPCs to make additional remote RPCs. (That is,
  if a read request came into server 1, server 1 might have to check server 3, the coordinator for that server.)

  So, while all non-E-PCI RPCs are cleanly implemented by overriding IKaijuRequest, E-PCI requests require their own
  special executor. :(

  This is a bit of a hack, but we decided to put basically all of this code into its own class to avoid polluting
  the remainder of the code base.
*/

public class EigerExecutor {
    private static Logger logger = LoggerFactory.getLogger(EigerExecutor.class);

    private RequestDispatcher dispatcher;
    private MemoryStorageEngine storageEngine;

    private ConcurrentMap<Long, EigerPendingTransaction> pendingTransactionsCoordinated = Maps.newConcurrentMap();
    private ConcurrentMap<Long, EigerPutAllRequest> pendingTransactionsNonCoordinated = Maps.newConcurrentMap();

    ReentrantLock pendingTransactionsLock = new ReentrantLock();
    private ConcurrentMap<String, Collection<Long>> pendingTransactionsPerKey = Maps.newConcurrentMap();

    // a roughly time-ordered queue of KVPs to GC; exact real-time ordering not necessary for correctness
    private BlockingQueue<CommittedGarbage> candidatesForGarbageCollection = Queues.newLinkedBlockingQueue();

    private static Histogram commitChecksNumKeys = MetricsManager.getRegistry().histogram(MetricRegistry.name(EigerExecutor.class,
                                                                                               "commit-check-num-keys",
                                                                                               "count"));

    private static Histogram commitCheckNumServers = MetricsManager.getRegistry().histogram(MetricRegistry.name(EigerExecutor.class,
                                                                                                 "commit-check-servers",
                                                                                                 "count"));

    private static Timer commitCheckReadTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(EigerExecutor.class,
                                                                                        "commit-check-read-timer",
                                                                                        "latency"));

    public EigerExecutor(RequestDispatcher dispatcher,
                         MemoryStorageEngine storageEngine) {
        this.dispatcher = dispatcher;
        this.storageEngine = storageEngine;

        new Thread(new Runnable() {
                    @Override
                    public void run() {
                        long currentTime = -1;
                        CommittedGarbage nextStamp = null;
                        while(true) {
                            try {
                                if(nextStamp == null)
                                    nextStamp = candidatesForGarbageCollection.take();
                                if(nextStamp.getExpirationTime() < currentTime ||
                                   (nextStamp.getExpirationTime() < (currentTime = System.currentTimeMillis())) ) {
                                    pendingTransactionsCoordinated.remove(nextStamp.getTimestamp());
                                    nextStamp = null;
                                } else {
                                    Thread.sleep(nextStamp.getExpirationTime()-currentTime);
                                }
                            } catch (InterruptedException e) {}
                        }
                    }
                }, "Eiger-GC-Thread").start();
    }

    public void processMessage(EigerPutAllRequest putAllRequest) throws KaijuException, IOException, InterruptedException {
        long transactionID = putAllRequest.keyValuePairs.values().iterator().next().getTimestamp();
        if(OutboundRouter.ownsResource(putAllRequest.coordinatorKey.hashCode())) {
            if(!pendingTransactionsCoordinated.containsKey(transactionID)) {
                pendingTransactionsCoordinated.putIfAbsent(transactionID, new EigerPendingTransaction());
            }

            pendingTransactionsCoordinated.get(transactionID).setCoordinatorState(putAllRequest.totalNumKeys,
                                                                                  putAllRequest.senderID,
                                                                                  putAllRequest.requestID);
        }

        assert(!pendingTransactionsNonCoordinated.containsKey(transactionID));
        pendingTransactionsNonCoordinated.put(transactionID, putAllRequest);

        for(String key : putAllRequest.keyValuePairs.keySet()) {
            markKeyPending(key, transactionID);
        }

        dispatcher.requestOneWay(putAllRequest.coordinatorKey.hashCode(), new EigerPreparedResponse(transactionID,
                                                                                                    putAllRequest
                                                                                                            .keyValuePairs
                                                                                                            .size(),
                                                                                                    Timestamp.assignNewTimestamp()));
    }

    private void commitEigerPendingTransaction(long transactionID, EigerPendingTransaction ept) throws IOException, InterruptedException{
        Map<Integer, KaijuMessage> toSend = Maps.newHashMap();
        for(int serverToNotify : ept.getServersToNotifyCommit()) {
            toSend.put(serverToNotify, new EigerCommitRequest(transactionID, ept.getCommitTime()));
        }

        dispatcher.multiRequestOneWay(toSend);

        dispatcher.sendResponse(ept.getClientID(), ept.getClientRequestID(), new KaijuResponse());

        candidatesForGarbageCollection.add(new CommittedGarbage(transactionID, System.currentTimeMillis()+Config.getConfig().overwrite_gc_ms));
    }

    public void processMessage(EigerPreparedResponse preparedNotification) throws KaijuException, IOException, InterruptedException {
        if(!pendingTransactionsCoordinated.containsKey(preparedNotification.transactionID)) {
            EigerPendingTransaction newTxn =new EigerPendingTransaction();
            pendingTransactionsCoordinated.putIfAbsent(preparedNotification.transactionID, newTxn);
        }

        EigerPendingTransaction ept = pendingTransactionsCoordinated.get(preparedNotification.transactionID);

        ept.recordPreparedKeys(preparedNotification.senderID, preparedNotification.numKeys, preparedNotification.preparedTime);

        if(ept.shouldCommit()) {
            commitEigerPendingTransaction(preparedNotification.transactionID, ept);
        }

    }

    public void processMessage(EigerCommitRequest commitNotification) throws KaijuException, IOException, InterruptedException {
        nonCoordinatorMarkCommitted(commitNotification.transactionID);
    }

    private void nonCoordinatorMarkCommitted(long transactionID) throws KaijuException {
        EigerPutAllRequest preparedRequest = pendingTransactionsNonCoordinated.get(transactionID);

        if(preparedRequest == null) {
            return;
        }

        Map<String, DataItem> toCommit = Maps.newHashMap();

        for(String key : preparedRequest.keyValuePairs.keySet()) {
            DataItem item = preparedRequest.keyValuePairs.get(key);
            toCommit.put(key, new DataItem(transactionID, item.getValue()));

            //logger.info(String.format("%d: COMMITTING %s [%s] at time %d\n", Config.getConfig().server_id, key, Arrays.toString(item.getValue().array()), commitNotification.commitTime));
        }

        storageEngine.putAll(toCommit);

        for(String key : preparedRequest.keyValuePairs.keySet()) {
            unmarkKeyPending(key, transactionID);
        }
    }

    public void processMessage(EigerGetAllRequest getAllRequest) throws KaijuException, IOException, InterruptedException {
        Timestamp.assignNewTimestamp(getAllRequest.readTimestamp);
        Map<String, DataItem> currentItems = Maps.newHashMap();
        Collection<Long> pendingTransactionsToCheck = Sets.newHashSet();
        Map<String, Set<Long>> pendingValuesToConsider = Maps.newHashMap();
        for(String key : getAllRequest.keys) {
            pendingValuesToConsider.put(key, new HashSet<Long>());
            Collection<Long> keyPending = checkKeyPending(key);
            if(keyPending == null) {
                continue;
            }

            for(Long pendingTransactionID : keyPending) {
                if(pendingTransactionID <= getAllRequest.readTimestamp) {

                    pendingTransactionsToCheck.add(pendingTransactionID);
                    pendingValuesToConsider.get(key).add(pendingTransactionID);
                }
            }
        }

        if(pendingTransactionsToCheck.isEmpty()) {
            commitChecksNumKeys.update(0);
            commitChecksNumKeys.update(0);
            for(String key : getAllRequest.keys) {
                currentItems.put(key, storageEngine.getHighestNotGreaterThan(key, getAllRequest.readTimestamp));
            }

            dispatcher.sendResponse(getAllRequest.senderID, getAllRequest.requestID, new KaijuResponse(currentItems));

            return;
        }

        Map<Integer, KaijuMessage> checkRequests = Maps.newHashMap();

        int commitCheckKeys = 0;

        Context startCheck = commitCheckReadTimer.time();

        for(Long pendingTransactionID : pendingTransactionsToCheck) {
            int serverIDForPendingTransaction = OutboundRouter.getRouter().getServerIDByResourceID(
                    pendingTransactionsNonCoordinated.get(pendingTransactionID).coordinatorKey.hashCode());
            if(!checkRequests.containsKey(serverIDForPendingTransaction))
                checkRequests.put(serverIDForPendingTransaction, new EigerCheckCommitRequest(getAllRequest.readTimestamp));

            ((EigerCheckCommitRequest)checkRequests.get(serverIDForPendingTransaction)).toCheck.add(pendingTransactionID);
            commitCheckKeys++;
        }

        commitChecksNumKeys.update(commitCheckKeys);
        commitCheckNumServers.update(checkRequests.size());

        Collection<KaijuResponse> responses = dispatcher.multiRequest(checkRequests);

        KaijuResponse.coalesceErrorsIntoException(responses);

        startCheck.stop();

        Map<Long, Long> convertUserTimestampToCommitTimestamp = Maps.newHashMap();

        for(KaijuResponse response : responses) {
            EigerCheckCommitResponse checkResponse = (EigerCheckCommitResponse) response;

            convertUserTimestampToCommitTimestamp.putAll(((EigerCheckCommitResponse) response).commitTimes);

            for(Map.Entry<Long, Long> result : checkResponse.commitTimes.entrySet()) {
                //remove any uncommitted pending transactions; we won't consider them...
                if(result.getValue() == -1 || result.getValue() > getAllRequest.readTimestamp) {
                    for(String transactionKey : pendingTransactionsNonCoordinated.get(result.getKey()).keyValuePairs.keySet()) {
                        Collection<Long> toConsider = pendingValuesToConsider.get(transactionKey);
                        if(toConsider != null)
                            toConsider.remove(result.getKey());
                    }
                }

                if(result.getValue() != -1) {
                    nonCoordinatorMarkCommitted(result.getValue());
                }
            }
        }

        for(String key : pendingValuesToConsider.keySet()) {
            Collection<Long> committedTransactionStamps = pendingValuesToConsider.get(key);
            if(committedTransactionStamps.isEmpty()) {
                continue;
            }

            long maxCommittedForKey = Collections.max(committedTransactionStamps);

            DataItem committedItem = pendingTransactionsNonCoordinated.get(maxCommittedForKey).keyValuePairs.get(key);
            currentItems.put(key, new DataItem(convertUserTimestampToCommitTimestamp.get(maxCommittedForKey), committedItem.getValue()));
        }

        for(String key : getAllRequest.keys) {
            DataItem committed = storageEngine.getHighestNotGreaterThan(key, getAllRequest.readTimestamp);
            if(!currentItems.containsKey(key) || committed.getTimestamp() > currentItems.get(key).getTimestamp()) {
                currentItems.put(key, committed);
            }
        }

        dispatcher.sendResponse(getAllRequest.senderID, getAllRequest.requestID, new KaijuResponse(currentItems));
    }

    public void processMessage(EigerCheckCommitRequest checkCommitRequest) throws KaijuException, IOException, InterruptedException {
        Timestamp.assignNewTimestamp();
        Map<Long, Long> ret = Maps.newHashMap();

        for(Long toCheck : checkCommitRequest.toCheck) {
            if(pendingTransactionsCoordinated.containsKey(toCheck)) {
                ret.put(toCheck, pendingTransactionsCoordinated.get(toCheck).hasCommitted() ?
                                 pendingTransactionsCoordinated.get(toCheck).getCommitTime() :
                                 -1L);

            }
            else
                ret.put(toCheck, -1L);
        }

        dispatcher.sendResponse(checkCommitRequest.senderID, checkCommitRequest.requestID, new EigerCheckCommitResponse(ret));
    }

    private void markKeyPending(String key, long timestamp) {
        pendingTransactionsLock.lock();
        if(!pendingTransactionsPerKey.containsKey(key)) {
            pendingTransactionsPerKey.put(key, new HashSet<Long>());
        }

        pendingTransactionsPerKey.get(key).add(timestamp);

        pendingTransactionsLock.unlock();
    }

    private void unmarkKeyPending(String key, long timestamp) {
        pendingTransactionsLock.lock();
        if(pendingTransactionsPerKey.get(key).size() == 1) {
            pendingTransactionsPerKey.remove(key);
        } else {
            pendingTransactionsPerKey.get(key).remove(timestamp);
        }
        pendingTransactionsLock.unlock();
    }

    private Collection<Long> checkKeyPending(String key) {
        Collection<Long> ret = Sets.newHashSet();
        pendingTransactionsLock.lock();
        Collection<Long> pending = pendingTransactionsPerKey.get(key);
        if(pending != null)
            ret.addAll(pending);
        pendingTransactionsLock.unlock();
        return ret;
    }

    class EigerPendingTransaction {
        private AtomicInteger numKeysSeen;
        private int numKeysWaiting;
        private Vector<Integer> serversToNotifyCommit = new Vector<Integer>();
        private int clientID = -1;
        private int clientRequestID = -1;
        AtomicBoolean readyToCommit = new AtomicBoolean(false);
        AtomicBoolean committed = new AtomicBoolean(false);

        private long highestPreparedTime = -1;

        ReentrantLock commitTimeLock = new ReentrantLock();

        public EigerPendingTransaction() {
            this.numKeysSeen = new AtomicInteger(0);
        }

        public void setCoordinatorState(int numKeysWaiting, int clientID, int clientRequestID) {
            this.numKeysWaiting = numKeysWaiting;
            this.clientID = clientID;
            this.clientRequestID = clientRequestID;
        }

        public synchronized boolean shouldCommit() {
            boolean ret = readyToCommit.getAndSet(false);
            if(ret)
                committed.set(true);
            return ret;
        }

        public synchronized boolean hasCommitted() {
            commitTimeLock.lock();
            if(!committed.get())
                highestPreparedTime = Timestamp.assignNewTimestamp(highestPreparedTime);
            commitTimeLock.unlock();

            return committed.get();
        }

        public long getCommitTime() {
            return highestPreparedTime;
        }

        public Collection<Integer> getServersToNotifyCommit() {
            return serversToNotifyCommit;
        }

        public int getClientID() {
            assert (clientID != -1);
            return clientID;
        }

        public int getClientRequestID() {
            assert (clientRequestID != -1);
            return clientRequestID;
        }

        public synchronized void recordPreparedKeys(int server, int numKeys, long preparedTime) {
            if(highestPreparedTime < preparedTime)
                highestPreparedTime = Timestamp.assignNewTimestamp(preparedTime);
            serversToNotifyCommit.add(server);
            numKeysSeen.getAndAdd(numKeys);

            if(numKeysSeen.get() == numKeysWaiting)
                readyToCommit.set(true);
        }

    }

    private class CommittedGarbage {
           private long timestamp;
           private long expirationTime = -1;

           public CommittedGarbage(long timestamp, long expirationTime) {
               this.timestamp = timestamp;
               this.expirationTime = expirationTime;
           }

           public long getExpirationTime(){
               return expirationTime;
           }

           private long getTimestamp() {
               return timestamp;
           }

           @Override
           public int hashCode() {
               return Long.valueOf(timestamp).hashCode();
           }

           @Override
           public boolean equals(Object obj) {
               if (obj == null)
                    return false;
                if (obj == this)
                    return true;
                if (!(obj instanceof CommittedGarbage))
                    return false;

                CommittedGarbage rhs = (CommittedGarbage) obj;
                return rhs.getTimestamp() == timestamp ;
           }
       }
}