package edu.berkeley.kaiju.service;


import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Maps;
import com.yammer.metrics.*;
import com.yammer.metrics.Timer.Context;
import edu.berkeley.kaiju.config.Config;
import edu.berkeley.kaiju.monitor.MetricsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Provides an interface for acquiring read and write locks.
 * Note that locks cannot be upgraded, so the caller must acquire the highest level of
 * lock needed initially, or else break 2PL.
 */
public class LockManager {
    private static Logger logger = LoggerFactory.getLogger(LockManager.class);

    private Lock tableLock = new ReentrantLock();

    protected static Counter blockedWriters = MetricsManager.getRegistry().counter(
            MetricRegistry.name(LockManager.class,
                                "blocked-writer",
                                "count"));

    protected static Counter blockedReaders = MetricsManager.getRegistry().counter(
            MetricRegistry.name(LockManager.class,
                                "blocked-reader",
                                "count"));

    protected static Meter blockedRequests = MetricsManager.getRegistry().meter(MetricRegistry.name(LockManager.class,
                                                                                                    "blocked-request",
                                                                                                    "count"));

    protected static Meter unblockedRequests = MetricsManager.getRegistry().meter(MetricRegistry.name(LockManager.class,
                                                                                                      "unblocked-request",
                                                                                                      "count"));

    protected static Histogram blockedQueueEntryLengths = MetricsManager.getRegistry().histogram(MetricRegistry.name(LockManager.class,
                                                                                        "blocked-queue",
                                                                                        "entrylength"));

    protected static Histogram blockedQueueWokenLengths = MetricsManager.getRegistry().histogram(MetricRegistry.name(LockManager.class,
                                                                                        "blocked-queue",
                                                                                        "wokenlength"));

    private Gauge<Integer> lockEntries = MetricsManager.getRegistry().register(MetricRegistry.name(LockManager.class,
                                                                                                   "locktable-entries",
                                                                                                   "count"),
                                                                               new Gauge<Integer>() {
                                                                                   @Override
                                                                                   public Integer getValue() {
                                                                                       return lockTable.size();
                                                                                   }
                                                                               });

    private static Timer writeLockTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(LockManager.class,
                                                                                                 "write-lock-time",
                                                                                                 "time"));

    private static Timer readLockTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(LockManager.class,
                                                                                                    "read-lock-time",
                                                                                                    "time"));

    public enum LockType { READ, WRITE }
    public enum LockDuration { LONG, SHORT };

    private static class LockRequest {
        private final LockType type;
        private final Condition condition;

        public LockRequest(LockType type, Condition condition) {
            this.type = type;
            this.condition = condition;
        }

        public void sleep() {
            try {
                condition.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void wake() {
            condition.signal();
        }
    }

    /**
     * Contains all the state related to a particular locked object.
     * This class supports multiple readers or a single writer.
     */
    private static class LockState {
        private boolean held;
        private LockType mode;
        private long numLockers;
        private AtomicLong numInterested;

        private Lock lockLock = new ReentrantLock();
        private Collection<LockRequest> queuedRequests;

        public LockState() {
            this.held = false;
            this.numLockers = 0;
            this.queuedRequests = new Vector<LockRequest>();
            this.numInterested = new AtomicLong();
        }

        public void markReference() {
            numInterested.incrementAndGet();
        }

        public void unmarkReference() {
            numInterested.decrementAndGet();
        }

        // should only be called with global table lock held as well
        public boolean canRemove() {
            return numInterested.get() == 0;
        }

        private boolean shouldGrantLock(LockRequest request) {
            // If no queued requests or no conflicting queued requests, we can
            // accept the request if it meshes with our R/W coexistence rules.
            return !held || (mode == LockType.READ && request.type == LockType.READ);
        }

        public boolean acquire(LockType wantType) {
            LockRequest request = new LockRequest(wantType, lockLock.newCondition());

            Context lockTimerContext = wantType == LockType.READ ? readLockTimer.time() : writeLockTimer.time();

            lockLock.lock();

            boolean blocked = false;

            try {
                if (!shouldGrantLock(request)) {
                    queuedRequests.add(request);

                    blockedRequests.mark();

                    blockedQueueEntryLengths.update(queuedRequests.size());

                    if(wantType == LockType.READ)
                        blockedReaders.inc();

                    if(wantType == LockType.WRITE)
                        blockedWriters.inc();

                    request.sleep();

                    queuedRequests.remove(request);

                    if(wantType == LockType.READ)
                        blockedReaders.dec();

                    if(wantType == LockType.WRITE)
                        blockedWriters.dec();
                } else {
                    unblockedRequests.mark();
                }

                held = true;
                numLockers += 1;
                mode = wantType;

                assert(numLockers == 1 || mode == LockType.READ);

                return true;
            } finally {
                lockLock.unlock();
                lockTimerContext.stop();
            }
        }

        public void release() {
            lockLock.lock();
            try {
                numLockers--;

                assert(numLockers == 0 || mode != LockType.WRITE);

                if (numLockers == 0) {
                    held = false;
                    wakeNextQueuedGroup();
                }
            } finally {
                lockLock.unlock();
            }
        }

        /** Wakes the next queued writer or consecutively queued readers. */
        private void wakeNextQueuedGroup() {
            boolean wokeReader = false;

            if(queuedRequests.size() > 0)
                blockedQueueWokenLengths.update(queuedRequests.size());

            for (LockRequest request : queuedRequests) {
                if (request.type == LockType.READ) {
                    request.wake();
                    wokeReader = true;
                } else if (request.type == LockType.WRITE) {
                    if (!wokeReader) {
                        request.wake();
                        break;
                    }
                }
            }
        }
    }

    private ConcurrentMap<String, LockState> lockTable;
    private List<Lock> tableLatches;
    private final int numLatches;

    private Lock getLatchForKey(String key) {
        return tableLatches.get(Math.abs(key.hashCode() % numLatches));
    }


    public LockManager() {
        lockTable = Maps.newConcurrentMap();

        numLatches = Config.getConfig().lock_table_num_latches;
        tableLatches = Lists.newArrayList(numLatches);
        for(int i = 0; i < numLatches; ++i) {
            tableLatches.add(new ReentrantLock());
        }
    }

    /**
     * Locks the key, blocking as necessary.
     */
     public void lock(LockType lockType, String key) {
        getLatchForKey(key).lock();
        if(!lockTable.containsKey(key))
            lockTable.put(key, new LockState());

        LockState lockState = lockTable.get(key);
        lockState.markReference();
        getLatchForKey(key).unlock();

        lockState.acquire(lockType);
    }

    public void unlock(String key) {
        LockState lockState = lockTable.get(key);
        lockState.release();
        lockState.unmarkReference();

        if(lockState.canRemove()) {
            getLatchForKey(key).lock();
            if(lockState.canRemove())
                lockTable.remove(key, lockState);
            getLatchForKey(key).unlock();
        }
    }

    public void unlock(Collection<String> keys) {
        for(String key : keys) {
            unlock(key);
        }
    }

    public void clear() {
        lockTable.clear();
    }
}