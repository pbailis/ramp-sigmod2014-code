package edu.berkeley.kaiju.net.tcp;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Uninterruptibles;
import edu.berkeley.kaiju.config.Config;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.util.KryoSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

/*
  Thread per connection server connection handler for outgoing requests.
  We separate the reader and the writer thread, though this isn't actually
  necessary.
 */
public class InternalTCPSender {
    private static Logger logger = LoggerFactory.getLogger(InternalTCPSender.class);

    private BlockingQueue<KaijuMessage> toSend = Queues.newLinkedBlockingQueue();

    public InternalTCPSender(final InetSocketAddress socketAddress) throws IOException {
        for(int i = 0; i < Config.getConfig().outbound_internal_conn; ++i) {
            try {
                final KryoSerializer serializer = new KryoSerializer();

                Socket remoteSocket = new Socket(socketAddress.getAddress(), socketAddress.getPort());

                if(Config.getConfig().tcp_nodelay) {
                    remoteSocket.setTcpNoDelay(true);
                }

                OutputStream outputStream = remoteSocket.getOutputStream();
                serializer.setOutputStream(outputStream);

                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        while (true) {

                            try {
                                serializer.serialize(Uninterruptibles.takeUninterruptibly(toSend));
                            } catch (IOException e) {
                                logger.error(String.format("Communication error with %s:%s",
                                                           socketAddress.getHostName(),
                                                           socketAddress.getPort()), e);
                            }
                        }
                    }
                }).start();
            } catch (IOException e) {
                throw new IOException("Error connecting to "+socketAddress, e);
            }
        }
    }

    public void enqueue(KaijuMessage message) {
        toSend.add(message);
    }
}