package edu.berkeley.kaiju.service.request;

import com.google.common.collect.Lists;
import com.yammer.metrics.Meter;
import com.yammer.metrics.MetricRegistry;
import com.yammer.metrics.Timer;
import edu.berkeley.kaiju.exception.KaijuException;
import edu.berkeley.kaiju.monitor.MetricsManager;
import edu.berkeley.kaiju.service.LockManager;
import edu.berkeley.kaiju.service.MemoryStorageEngine;
import edu.berkeley.kaiju.service.request.eiger.EigerExecutor;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.request.*;
import edu.berkeley.kaiju.service.request.message.response.EigerPreparedResponse;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RequestExecutor implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(RequestExecutor.class);

    private static Timer getTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(RequestExecutor.class,
                                                                            "get-requests",
                                                                            "latencies"));

    private static  Timer putTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(RequestExecutor.class,
                                                                            "put-requests",
                                                                            "latencies"));

    private static  Timer getAllTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(RequestExecutor.class,
                                                                               "get-all-requests",
                                                                               "latencies"));

    private static  Timer putAllTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(RequestExecutor.class,
                                                                               "put-all-requests",
                                                                               "latencies"));

    private static  Timer getTimestampsTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(RequestExecutor.class,
                                                                               "get-timestamps-requests",
                                                                               "latencies"));

    private static Timer getAllByTimestampTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(RequestExecutor.class,
                                                                               "get-all-by-timestamp",
                                                                               "latencies"));

    private static Timer getAllByTimestampListTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(RequestExecutor.class,
                                                                               "get-all-by-timestamp-list",
                                                                               "latencies"));

    private static Timer getEachByTimestampListTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(RequestExecutor.class,
                                                                               "get-each-by-timestamp-list",
                                                                               "latencies"));

    private static Timer preparePutAllTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(RequestExecutor.class,
                                                                                      "prepare-put-all-requests",
                                                                                      "latencies"));

    private static Timer commitPutAllTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(RequestExecutor.class,
                                                                                     "commit-put-all-requests",
                                                                                     "latencies"));

    private static Timer readLockTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(RequestExecutor.class,
                                                                             "read-lock-requests",
                                                                             "latencies"));

    private static Timer writeLockTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(RequestExecutor.class,
                                                                             "write-lock-requests",
                                                                             "latencies"));

    private static Timer unlockTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(RequestExecutor.class,
                                                                             "unlock-requests",
                                                                             "latencies"));

    private static Timer eigerCheckCommitTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(RequestExecutor.class,
                                                                             "eiger-check-commit-requests",
                                                                             "latencies"));

    private static Timer eigerCommitTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(RequestExecutor.class,
                                                                             "eiger-commit-requests",
                                                                             "latencies"));

    private static Timer eigerGetAllTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(RequestExecutor.class,
                                                                             "eiger-getall-requests",
                                                                             "latencies"));

    private static Timer eigerPutAllTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(RequestExecutor.class,
                                                                             "eiger-getall-requests",
                                                                             "latencies"));

    private static Timer eigerPreparedTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(RequestExecutor.class,
                                                                             "eiger-prepared-responses",
                                                                             "latencies"));

    private static Timer checkPreparedTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(RequestExecutor.class,
                                                                             "check-prepared-responses",
                                                                             "latencies"));

    private static Meter errorMeter = MetricsManager.getRegistry().meter(MetricRegistry.name(RequestExecutor.class,
                                                                              "errors",
                                                                              "count"));

    private RequestDispatcher dispatcher;
    private MemoryStorageEngine storageEngine;
    private KaijuMessage message;
    private LockManager lockManager;
    private EigerExecutor eigerExecutor;

    public RequestExecutor(RequestDispatcher dispatcher,
                           MemoryStorageEngine storageEngine,
                           LockManager lockManager,
                           KaijuMessage message,
                           EigerExecutor eigerExecutor) {
        this.dispatcher = dispatcher;
        this.storageEngine = storageEngine;
        this.lockManager = lockManager;
        this.message = message;
        this.eigerExecutor = eigerExecutor;
    }

    @Override
    public void run() {
        Timer.Context context;

        if(message instanceof GetRequest) {
            context = getTimer.time();
        } else if(message instanceof PutRequest) {
            context = putTimer.time();
        } else if(message instanceof GetAllRequest) {
            context = getAllTimer.time();
        } else if(message instanceof PutAllRequest) {
            context = putAllTimer.time();
        } else if(message instanceof GetTimestampsRequest) {
            context = getTimestampsTimer.time();
        } else if(message instanceof GetAllByTimestampRequest) {
            context = getAllByTimestampTimer.time();
        } else if(message instanceof GetAllByTimestampListRequest) {
            context = getAllByTimestampListTimer.time();
        } else if(message instanceof PreparePutAllRequest) {
            context = preparePutAllTimer.time();
        } else if(message instanceof CommitPutAllRequest) {
            context = commitPutAllTimer.time();
        } else if(message instanceof ReadLockRequest) {
            context = readLockTimer.time();
        } else if(message instanceof WriteLockRequest) {
            context = writeLockTimer.time();
        } else if(message instanceof UnlockRequest) {
            context = unlockTimer.time();
        } else if(message instanceof GetEachByTimestampListRequest) {
            context = getEachByTimestampListTimer.time();
        } else if(message instanceof CheckPreparedRequest) {
            context = checkPreparedTimer.time();
        }
        // must be an Eiger request; see discsussion in EigerExecutor.
        else {
            try {
                if(message instanceof EigerCheckCommitRequest) {
                    context = eigerCheckCommitTimer.time();
                    eigerExecutor.processMessage((EigerCheckCommitRequest) message);
                    context.stop();
                    return;
                } else if (message instanceof EigerCommitRequest) {
                    context = eigerCommitTimer.time();
                    eigerExecutor.processMessage((EigerCommitRequest) message);
                    context.stop();
                    return;
                } else if (message instanceof EigerGetAllRequest) {
                    context = eigerGetAllTimer.time();
                    eigerExecutor.processMessage((EigerGetAllRequest) message);
                    context.stop();
                    return;
                } else if (message instanceof EigerPutAllRequest) {
                    context = eigerPutAllTimer.time();
                    eigerExecutor.processMessage((EigerPutAllRequest) message);
                    context.stop();
                    return;
                } else if (message instanceof EigerPreparedResponse) {
                    context = eigerPreparedTimer.time();
                    eigerExecutor.processMessage((EigerPreparedResponse) message);
                    context.stop();
                    return;
                }
            } catch(Exception e) {
                logger.error("EIGER ERROR", e);
            }

            throw new RuntimeException("No known message type: "+message.getClass());
        }

        KaijuResponse response;

        try {
            response = ((IKaijuRequest) message).processRequest(storageEngine, lockManager);
        } catch(KaijuException e) {
            errorMeter.mark();
            response = new KaijuResponse();
            response.errors = Lists.newArrayList(e.getMessage());
        }

        context.stop();

        try {
            dispatcher.sendResponse(message.senderID, message.requestID, response);
        } catch(IOException e) {
            logger.error("Error sending response: ", e);
        }
    }
}