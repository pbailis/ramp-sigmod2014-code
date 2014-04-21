
package edu.berkeley.kaiju;

import edu.berkeley.kaiju.config.Config;
import edu.berkeley.kaiju.frontend.FrontendServer;
import edu.berkeley.kaiju.monitor.MetricsManager;
import edu.berkeley.kaiju.net.InboundMessagingService;
import edu.berkeley.kaiju.net.routing.OutboundRouter;
import edu.berkeley.kaiju.service.CooperativeCommitter;
import edu.berkeley.kaiju.service.LockManager;
import edu.berkeley.kaiju.service.MemoryStorageEngine;
import edu.berkeley.kaiju.service.request.RequestDispatcher;
import edu.berkeley.kaiju.service.request.RequestExecutorFactory;
import edu.berkeley.kaiju.service.request.eiger.EigerExecutor;
import edu.berkeley.kaiju.service.request.handler.KaijuServiceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KaijuServer {

    private static Logger logger = LoggerFactory.getLogger(KaijuServer.class);

    public static void main(String[] args) {
        Config.serverSideInitialize(args);

        MetricsManager.initializeMetrics();

        MemoryStorageEngine storage = new MemoryStorageEngine();
        LockManager lockManager = new LockManager();
        RequestExecutorFactory requestExecutorFactory = new RequestExecutorFactory(storage, lockManager);
        RequestDispatcher dispatcher = new RequestDispatcher(requestExecutorFactory);
        requestExecutorFactory.setEigerExecutor(new EigerExecutor(dispatcher, storage));
        new CooperativeCommitter(storage, new KaijuServiceHandler(dispatcher, storage, lockManager));

        try {
            InboundMessagingService.start(dispatcher);
        } catch (IOException e) {
            logger.error("Error starting inbound messaging service", e);
            System.exit(-1);
        }

        logger.info("Started listening for connections...");

        try {
            Thread.sleep(Config.getConfig().bootstrap_time);
        } catch (InterruptedException e) {
            logger.warn("Bootstrap interrupted", e);
        }

        try {
            OutboundRouter.initializeRouter();
        } catch (IOException e) {
            logger.error("Error starting outbound messaging service", e);
            System.exit(-1);
        }

        logger.info("Initialized Kaiju internal services; starting Thrift server");

        KaijuServiceHandler handler = new KaijuServiceHandler(dispatcher, storage, lockManager);

        try {
            new FrontendServer(handler, Config.getConfig().thrift_port).serve();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
