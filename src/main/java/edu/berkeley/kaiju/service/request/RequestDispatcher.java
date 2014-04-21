package edu.berkeley.kaiju.service.request;

import com.google.common.collect.Maps;
import edu.berkeley.kaiju.config.Config;
import edu.berkeley.kaiju.net.callback.IMessageCallback;
import edu.berkeley.kaiju.net.callback.MultiMessageCallback;
import edu.berkeley.kaiju.net.callback.SingleMessageCallback;
import edu.berkeley.kaiju.net.routing.OutboundRouter;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/*
  Handles dispatching of messages to appropriate servers and triggers appropriate callbacks on message receive.
 */

public class RequestDispatcher {
    private static Logger logger = LoggerFactory.getLogger(RequestDispatcher.class);

    private AtomicInteger requestIDGenerator = new AtomicInteger();
    private Map<Integer, edu.berkeley.kaiju.net.callback.IMessageCallback> outstandingRequests = Maps.newConcurrentMap();

    /*
     The implementation in this paper uses a cached thread pool. In later work, I've switched  over to using
     a fixed thread pool with non-blocking RPC handlers. The upside of the cached thread pool is that RPC
     handlers can block (i.e., no need for stack-ripping).
     The downside is that, with a lot of threads, the JVM performance can really suck.
     This was faster than using Thrift; YMMV. But it's what what we did for the paper.
     */
    private ExecutorService requestExecutor = Executors.newCachedThreadPool();
    private RequestExecutorFactory executorFactory;

    public RequestDispatcher(RequestExecutorFactory executorFactory) {
        this.executorFactory = executorFactory;
    }

    /*
      Request/response send/receive
     */

    private KaijuResponse request(int resourceID, KaijuMessage message, SingleMessageCallback callback) throws InterruptedException, IOException {
        int requestID = requestIDGenerator.incrementAndGet();

        message.senderID = Config.getConfig().server_id;
        message.requestID = requestID;

        if(callback != null)
            outstandingRequests.put(requestID, callback);
        OutboundRouter.getRouter().getChannelByResourceID(resourceID)
                     .enqueue(message);

        if(callback == null)
            return null;

        return callback.blockForResponse();
    }

    public KaijuResponse request(int resourceID, KaijuMessage message) throws InterruptedException, IOException {
        return request(resourceID, message, new SingleMessageCallback());
    }

    public void requestOneWay(int resourceID, KaijuMessage message) throws InterruptedException, IOException {
        request(resourceID, message, null);
    }

    private Collection<KaijuResponse> multiRequest(Map<Integer, KaijuMessage> requests, MultiMessageCallback callback)
            throws InterruptedException, IOException {
        for(Map.Entry<Integer, KaijuMessage> request : requests.entrySet()) {
            int requestID = requestIDGenerator.incrementAndGet();

            if(callback != null)
                outstandingRequests.put(requestID, callback);
            request.getValue().senderID = Config.getConfig().server_id;
            request.getValue().requestID = requestID;

            OutboundRouter.getRouter().getChannelByResourceID(request.getKey())
                          .enqueue(request.getValue());
        }

        if(callback == null)
            return null;

        return callback.blockForResponses();
    }

    // requests is map from serverID to requests
    public Collection<KaijuResponse> multiRequest(Map<Integer, KaijuMessage> requests) throws InterruptedException, IOException {
        return multiRequest(requests, new MultiMessageCallback(requests.size()));
    }

    public Collection<KaijuResponse> multiRequestBlockFor(Map<Integer, KaijuMessage> requests, int blockFor) throws InterruptedException, IOException {
        return multiRequest(requests, new MultiMessageCallback(blockFor));
    }

        // requests is map from serverID to requests
    public void multiRequestOneWay(Map<Integer, KaijuMessage> requests) throws InterruptedException, IOException {
        multiRequest(requests, null);
    }

    private void deliverResponse(KaijuMessage inboundResponse) {
        IMessageCallback callback = outstandingRequests.remove(inboundResponse.requestID);

        /*
        if(callback == null) {
            logger.error("Received callback for message ID "+
                         inboundResponse.requestID+
                         "(type: "+inboundResponse.getClass()+")"+
                         "(sender: "+inboundResponse.senderID+")"+
                         " but no matching request found");
            return;
        }
        */

        callback.notifyResponse(inboundResponse);
    }

    /*
      Send response to remotely originating request
     */

    public void sendResponse(int recipientID, int requestID, KaijuMessage message) throws IOException {
        message.senderID = Config.getConfig().server_id;
        message.requestID = requestID;
        OutboundRouter.getRouter().getChannelByServerID(recipientID).enqueue(message);
    }

    /*
      Differentiate between incoming requests and incoming responses; process incoming responses
     */

    public void processInbound(KaijuMessage message) {
        // request originated here
        if(message.isResponse()) {
            deliverResponse(message);
        } else {
            requestExecutor.execute(executorFactory.createExecutor(this, message));
        }
    }

    public void clear() {
        outstandingRequests.clear();
    }
}