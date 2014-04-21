package edu.berkeley.kaiju.service.request.handler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.berkeley.kaiju.config.Config;
import edu.berkeley.kaiju.data.DataItem;
import edu.berkeley.kaiju.data.ItemVersion;
import edu.berkeley.kaiju.exception.HandlerException;
import edu.berkeley.kaiju.net.routing.OutboundRouter;
import edu.berkeley.kaiju.service.request.RequestDispatcher;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.request.*;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;
import edu.berkeley.kaiju.util.Timestamp;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

/*
 Since the RAMP protocols all have the same write logic, ReadAtomicKaijuServiceHandler provides functionality
 for writes (delegating to the abstract instantiateKaijuItem method for attaching metadata).
  */

public abstract class ReadAtomicKaijuServiceHandler implements IKaijuHandler {
    RequestDispatcher dispatcher;

    private Random random = new Random();
    private float dropCommitPercentage = Config.getConfig().drop_commit_pct;

    public ReadAtomicKaijuServiceHandler(RequestDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    // Different RA algorithms handle get_all differently based on metadata
    public abstract Map<String, byte[]> get_all(List<String> keys) throws HandlerException;

    protected Collection<KaijuResponse> fetch_from_server(List<String> keys) throws HandlerException {
        return fetch_from_server(keys, null, null, null, false);
    }

    protected Collection<KaijuResponse> fetch_stamps_from_server(List<String> keys) throws HandlerException {
        return fetch_from_server(keys, null, null, null, true);
    }


    // fetch specific versions from server (e.g., second-round RAMP-Fast)
    protected Collection<KaijuResponse> fetch_by_version_from_server(Map<String, Long> keyToVersionRequired) throws HandlerException {
        return fetch_from_server(null, keyToVersionRequired, null, null, false);
    }

    // fetch versions from server based on list of possible timestamps (e.g., second-round RAMP-Hybrid)
    protected Collection<KaijuResponse> fetch_keys_by_version_lists(
            Map<String, Collection<Long>> keysToVersionsRequested) throws HandlerException {
        return fetch_from_server(null, null, keysToVersionsRequested, null, false);
    }

    // fetch versions from server based on timestamp list (e.g., second-round RAMP-Stamp)
    protected Collection<KaijuResponse> fetch_by_stamplist_from_server(Collection<String> keys, Collection<Long> stamps) throws HandlerException {
        return fetch_from_server(keys, null, null, stamps, false);
    }

    // Different RA algorithms will instantiate the KaijuItem with different metadata
    public abstract DataItem instantiateKaijuItem(byte[] value,
                                                  Collection<String> allKeys,
                                                  long timestamp);

    public void put_all(Map<String, byte[]> keyValuePairs) throws HandlerException {
        try {
            // generate a timestamp for this transaction
            long timestamp = Timestamp.assignNewTimestamp();

            // group keys by responsible server.
            Map<Integer, Collection<String>> keysByServerID = OutboundRouter.getRouter().groupKeysByServerID(keyValuePairs.keySet());
            Map<Integer, KaijuMessage> requestsByServerID = Maps.newHashMap();

            for(int serverID : keysByServerID.keySet()) {
                Map<String, DataItem> keyValuePairsForServer = Maps.newHashMap();
                for(String key : keysByServerID.get(serverID)) {
                    keyValuePairsForServer.put(key, instantiateKaijuItem(keyValuePairs.get(key),
                                                                         keyValuePairs.keySet(),
                                                                         timestamp));
                }

                requestsByServerID.put(serverID, new PreparePutAllRequest(keyValuePairsForServer));
            }

            // execute the prepare phase and check for errors
            Collection<KaijuResponse> responses = dispatcher.multiRequest(requestsByServerID);
            KaijuResponse.coalesceErrorsIntoException(responses);

            requestsByServerID.clear();
            for(int serverID : keysByServerID.keySet()) {
                requestsByServerID.put(serverID,  new CommitPutAllRequest(timestamp));
            }

            // this is only for the experiment in Section 5.3 and will trigger CTP
            if(dropCommitPercentage != 0 && random.nextFloat() < dropCommitPercentage) {
                int size = keysByServerID.size();
                int item = random.nextInt(size);
                int i = 0;
                for(int serverID : keysByServerID.keySet())
                {
                    if (i == item) {
                        requestsByServerID.remove(serverID);
                        break;
                    }

                    i++;
                }

                if(requestsByServerID.isEmpty()) {
                    return;
                }
            }

            responses = dispatcher.multiRequest(requestsByServerID);

            KaijuResponse.coalesceErrorsIntoException(responses);
        } catch (Exception e) {
            throw new HandlerException("Error processing request", e);
        }
    }

    // This is terrible. But basically it implements the above methods.
    private Collection<KaijuResponse> fetch_from_server(Collection<String> keyList,
                                                        Map<String, Long> keyToVersionRequired,
                                                        Map<String, Collection<Long>> keyToVersionsRequested,
                                                        Collection<Long> versionList,
                                                        boolean onlyReturnVersions) throws HandlerException {
        try {
            Collection<String> allKeys = keyList;

            if(allKeys == null && keyToVersionRequired != null) {
                allKeys = Lists.newArrayList(keyToVersionRequired.keySet());
            } else if(allKeys == null && keyToVersionsRequested != null) {
                allKeys = Lists.newArrayList(keyToVersionsRequested.keySet());
            }

            Map<Integer, Collection<String>> keysByServerID = OutboundRouter.getRouter().groupKeysByServerID(allKeys);
            Map<Integer, KaijuMessage> requestsByServerID = Maps.newHashMap();

            for(int serverID : keysByServerID.keySet()) {
                // fetch stamps from server
                if(onlyReturnVersions) {
                    requestsByServerID.put(serverID, new GetTimestampsRequest(keysByServerID.get(serverID)));
                }
                // vanilla get-all
                else if(keyList != null && versionList == null) {
                    requestsByServerID.put(serverID,  new GetAllRequest(keysByServerID.get(serverID)));
                }
                // get-all by version
                else if (keyToVersionRequired != null) {
                    Collection<ItemVersion> serverKeyVersionsRequired = Lists.newArrayList();
                    for(String key : keysByServerID.get(serverID)) {
                        serverKeyVersionsRequired.add(new ItemVersion(key, keyToVersionRequired.get(key)));
                    }

                    requestsByServerID.put(serverID, new GetAllByTimestampRequest(serverKeyVersionsRequired));
                } else if (keyToVersionsRequested != null) {
                    Map<String, Collection<Long>> serverKeyVersionsRequired = Maps.newHashMap();
                    for(String key : keysByServerID.get(serverID)) {
                        serverKeyVersionsRequired.put(key, keyToVersionsRequested.get(key));
                    }

                    requestsByServerID.put(serverID, new GetEachByTimestampListRequest(serverKeyVersionsRequired));
                }
                // get-all by version list
                else {
                    requestsByServerID.put(serverID, new GetAllByTimestampListRequest(keysByServerID.get(serverID), versionList));
                }
            }

            Collection<KaijuResponse> responses = dispatcher.multiRequest(requestsByServerID);

            KaijuResponse.coalesceErrorsIntoException(responses);

            return responses;
        } catch(Exception e) {
            throw new HandlerException("Error processing request", e);
        }
    }
}