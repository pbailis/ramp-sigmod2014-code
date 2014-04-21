package edu.berkeley.kaiju.net.routing;

import java.io.IOException;

public class HashingRouter extends OutboundRouter {
    public HashingRouter() throws IOException {
        super();
    }

    @Override
    public int getServerIDByResourceID(int resourceID) {
        return Math.abs(resourceID) % senders.size();
    }
}