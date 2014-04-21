package edu.berkeley.kaiju.net.routing;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.berkeley.kaiju.config.Config;
import edu.berkeley.kaiju.net.tcp.InternalTCPSender;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/*
 "Maintains" the consistent hashing for the cluster.
 With an all-to-all topology, we simply have to look up a partition by item.
 FWIW, every item has an integer "resource ID"; for strings, their HashCode.

 Replication is not supported.
 */
public abstract class OutboundRouter {
    protected static List<InternalTCPSender> senders = Lists.newArrayList();

    private static OutboundRouter router = null;

    public abstract int getServerIDByResourceID(int resourceID);

    public InternalTCPSender getChannelByResourceID(int resourceID) {
        return senders.get(getServerIDByResourceID(resourceID));
    }

    public static InternalTCPSender getChannelByServerID(int serverID) {
        return senders.get(serverID);
    }

    public OutboundRouter() throws IOException {
        for(InetSocketAddress serverAddress : Config.getConfig().cluster_servers) {
            senders.add(new InternalTCPSender(serverAddress));
        }
    }

    public static void initializeRouter() throws IOException {
        Config.RoutingMode mode = Config.getConfig().routing_strategy;
        router = new HashingRouter();
    }

    public static OutboundRouter getRouter() {
        return router;
    }

    public static boolean ownsResource(int resourceID) {
        return router.getServerIDByResourceID(resourceID) == Config.getConfig().server_id;
    }

    /*
     This is a very useful function.
     Given a set of keys (Strings), it groups the keys by the partition ID of the server
     that is responsible for each. Among other things, we use this for batching reads/writes that
     are sent to the same server (partition).
    */
    public Map<Integer, Collection<String>> groupKeysByServerID(Collection<String> keys) {
        Map<Integer, Collection<String>> ret = Maps.newHashMap();
        for(String key : keys) {
            int serverID = getServerIDByResourceID(key.hashCode());
            if(!ret.containsKey(serverID))
                ret.put(serverID,  new ArrayList<String>());
            ret.get(serverID).add(key);
        }

        return ret;
    }
}