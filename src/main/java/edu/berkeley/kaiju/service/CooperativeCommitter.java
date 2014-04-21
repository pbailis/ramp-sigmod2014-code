package edu.berkeley.kaiju.service;

import com.yammer.metrics.Meter;
import com.yammer.metrics.MetricRegistry;
import com.yammer.metrics.Timer;
import com.yammer.metrics.Timer.Context;
import edu.berkeley.kaiju.config.Config;
import edu.berkeley.kaiju.exception.HandlerException;
import edu.berkeley.kaiju.exception.KaijuException;
import edu.berkeley.kaiju.monitor.MetricsManager;
import edu.berkeley.kaiju.service.request.handler.KaijuServiceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
 Contains code for the CTP implementation (Section 4.6, 5.3)
 */
public class CooperativeCommitter {
    private static final Logger logger = LoggerFactory.getLogger(CooperativeCommitter.class);
    private static MemoryStorageEngine storageEngine;
    private static KaijuServiceHandler handler;

    private static Timer commitCheckTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(CooperativeCommitter.class,
                                                                                                  "commit-check-latency",
                                                                                                  "latencies"));

    private static Meter completedCommitMeter = MetricsManager.getRegistry().meter(MetricRegistry.name(CooperativeCommitter.class,
                                                                                                       "successful-commits",
                                                                                                       "count"));

    private static Meter completedAbortMeter = MetricsManager.getRegistry().meter(MetricRegistry.name(CooperativeCommitter.class,
                                                                                                           "aborted-commits",
                                                                                                           "count"));

    private static final Set checkedStamps = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());

    public CooperativeCommitter(final MemoryStorageEngine storageEngine,
                                final KaijuServiceHandler handler) {

        this.storageEngine = storageEngine;
        this.handler = handler;

        final int checkCommitDelay = Config.getConfig().check_commit_delay_ms;
        final ExecutorService executor = Executors.newCachedThreadPool();

        if(checkCommitDelay > 0) {
            new Thread(new Runnable() {
                       @Override
                       public void run() {

                           while(true) {
                               try {
                                   long latestDelta = Long.MAX_VALUE;
                                   long now = ((System.currentTimeMillis() << 26) >> 26);

                                   for(long timestamp : storageEngine.getPendingStamps()) {
                                       long delta = now - (timestamp >> 26);

                                       if(delta > checkCommitDelay && !checkedStamps.contains(timestamp)) {
                                           checkedStamps.add(timestamp);
                                           executor.submit(new CommitFinalizer(timestamp));

                                       } else if(delta < latestDelta) {
                                           latestDelta = Math.max(0, delta);
                                       }
                                   }

                                   if(latestDelta == Long.MAX_VALUE) {
                                       Thread.sleep(1000);
                                   } else {
                                       Thread.sleep(checkCommitDelay);
                                   }

                               } catch (InterruptedException e) {}
                           }
                       }
                   }, "Storage-Finalize-Commit-Thread").start();
        }
    }

    private class CommitFinalizer implements Runnable {
        long timestamp;

        public CommitFinalizer(long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public void run() {
            Context starttime = commitCheckTimer.time();

            try {
                boolean shouldCommit = handler.checkCommitted(timestamp, storageEngine.getPendingKeys(timestamp));

                if(shouldCommit) {
                    completedCommitMeter.mark();
                    storageEngine.commit(timestamp);
                } else {
                    completedAbortMeter.mark();
                    storageEngine.abort(timestamp);
                }

                checkedStamps.remove(timestamp);
            } catch (HandlerException e) {
                logger.warn("Committer Handler exception ", e);
            } catch (KaijuException e) {
                logger.warn("Committer Kaiju exception ", e);
            } finally {
                starttime.stop();
            }
        }
    }
}