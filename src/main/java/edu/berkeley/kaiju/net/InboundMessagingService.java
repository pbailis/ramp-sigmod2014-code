package edu.berkeley.kaiju.net;

import edu.berkeley.kaiju.config.Config;
import edu.berkeley.kaiju.net.tcp.InternalTCPReceiver;
import edu.berkeley.kaiju.service.request.RequestDispatcher;

import java.io.IOException;

/*
 Probably didn't need an entire class for this.
 */
public class InboundMessagingService {
    public static void start(RequestDispatcher dispatcher) throws IOException {
        new InternalTCPReceiver(Config.getConfig().kaiju_port, dispatcher);
    }
}