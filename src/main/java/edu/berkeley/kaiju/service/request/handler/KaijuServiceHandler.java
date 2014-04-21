package edu.berkeley.kaiju.service.request.handler;

import com.google.common.collect.Maps;
import com.yammer.metrics.MetricRegistry;
import com.yammer.metrics.Timer;
import edu.berkeley.kaiju.config.Config;
import edu.berkeley.kaiju.config.Config.ReadAtomicAlgorithm;
import edu.berkeley.kaiju.data.DataItem;
import edu.berkeley.kaiju.exception.HandlerException;
import edu.berkeley.kaiju.frontend.request.ClientGetAllRequest;
import edu.berkeley.kaiju.frontend.request.ClientPutAllRequest;
import edu.berkeley.kaiju.frontend.request.ClientRequest;
import edu.berkeley.kaiju.frontend.request.ClientSetIsolationRequest;
import edu.berkeley.kaiju.frontend.response.ClientGetAllResponse;
import edu.berkeley.kaiju.frontend.response.ClientPutAllResponse;
import edu.berkeley.kaiju.frontend.response.ClientResponse;
import edu.berkeley.kaiju.monitor.MetricsManager;
import edu.berkeley.kaiju.net.routing.OutboundRouter;
import edu.berkeley.kaiju.service.LockManager;
import edu.berkeley.kaiju.service.MemoryStorageEngine;
import edu.berkeley.kaiju.service.request.RequestDispatcher;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.request.CheckPreparedRequest;
import edu.berkeley.kaiju.service.request.message.request.PutAllRequest;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;
import edu.berkeley.kaiju.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class KaijuServiceHandler implements IKaijuHandler {
    private IKaijuHandler handler;
    private RequestDispatcher dispatcher;
    private LockManager manager;
    private MemoryStorageEngine storage;

    private static Logger logger = LoggerFactory.getLogger(KaijuServiceHandler.class);

    private static Timer getAllTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(KaijuServiceHandler.class,
                                                                                       "getall-requests",
                                                                                       "latency"));

    private static Timer putAllTimer = MetricsManager.getRegistry().timer(MetricRegistry.name(KaijuServiceHandler.class,
                                                                                       "putall-requests",
                                                                                       "latency"));

    public KaijuServiceHandler(RequestDispatcher dispatcher, MemoryStorageEngine storage, LockManager manager) {
        this.dispatcher = dispatcher;
        this.manager = manager;
        this.storage = storage;
        setHandler();
    }

    private void setHandler() {
        switch (Config.getConfig().isolation_level) {
            case READ_COMMITTED:
                handler = new ReadCommittedKaijuServiceHandler(dispatcher);
                break;
            case READ_ATOMIC:
                if (Config.getConfig().readatomic_algorithm == ReadAtomicAlgorithm.KEY_LIST) {
                    handler = new ReadAtomicListBasedKaijuServiceHandler(dispatcher);
                } else if (Config.getConfig().readatomic_algorithm == ReadAtomicAlgorithm.TIMESTAMP) {
                    handler = new ReadAtomicStampBasedKaijuServiceHandler(dispatcher);
                } else if (Config.getConfig().readatomic_algorithm == ReadAtomicAlgorithm.BLOOM_FILTER) {
                    handler = new ReadAtomicBloomBasedKaijuServiceHandler(dispatcher);
                }
                break;
            case LWLR:
            case LWSR:
            case LWNR:
                handler = new LockBasedKaijuServiceHandler(dispatcher);
                break;
            case EIGER:
                handler = new EigerKaijuServiceHandler(dispatcher);
                break;
            default:
                throw new RuntimeException("No handler defined!");
        }
    }

    public ClientResponse processRequest(ClientRequest request) throws HandlerException {

        if(request instanceof ClientGetAllRequest) {
            Map<String, byte[]> ret = get_all(((ClientGetAllRequest) request).keys);
            return new ClientGetAllResponse(ret);
        } else if (request instanceof ClientPutAllRequest) {
            put_all(((ClientPutAllRequest) request).keyValuePairs);
            return new ClientPutAllResponse();
        } else if (request instanceof ClientSetIsolationRequest) {
            ClientSetIsolationRequest isolationRequest = (ClientSetIsolationRequest) request;
            Config.getConfig().isolation_level = isolationRequest.isolationLevel;
            Config.getConfig().readatomic_algorithm = isolationRequest.readAtomicAlgorithm;
            setHandler();
            storage.reset();
            manager.clear();
            dispatcher.clear();
        }

        return null;
    }

    public Map<String, byte[]> get_all(List<String> keys) throws HandlerException {
        Timer.Context context = getAllTimer.time();
        try {
            return handler.get_all(keys);
	} catch(HandlerException e) {
	    logger.warn("get_all exception", e);
	    throw e;
	} finally {
            context.stop();
        }
    }

    public void put_all(Map<String, byte[]> values) throws HandlerException {
        Timer.Context context = putAllTimer.time();
        try {
            handler.put_all(values);
	} catch(HandlerException e) {
	    logger.warn("put_all exception", e);
	    throw e;
	} finally {
            context.stop();
        }
    }

    // used in CTP; probably shouldn't actually live here.
    public boolean checkCommitted(long timestamp, Collection<String> keys) throws HandlerException {
        Map<Integer, Collection<String>> keysByServerID = OutboundRouter.getRouter().groupKeysByServerID(keys);
        Map<Integer, KaijuMessage> requestsByServerID = Maps.newHashMap();

       for(int serverID : keysByServerID.keySet()) {
           if(serverID != Config.getConfig().server_id) {
               requestsByServerID.put(serverID, new CheckPreparedRequest(timestamp, keysByServerID.get(serverID)));
           }
       }

        if(requestsByServerID.isEmpty()) {
            return true;
        }

        try {
           Collection<KaijuResponse> responses = dispatcher.multiRequest(requestsByServerID);
           KaijuResponse.coalesceErrorsIntoException(responses);


            for(KaijuResponse response : responses) {
                if(!response.prepared) {
                    return false;
                }
            }
       } catch (Exception e) {
           throw new HandlerException("Error processing request", e);
       }

        return true;
    }
}
