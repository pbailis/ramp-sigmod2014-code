package edu.berkeley.kaiju.net.tcp;

import com.esotericsoftware.kryo.KryoException;
import edu.berkeley.kaiju.service.request.RequestDispatcher;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.util.KryoSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

/*
  Thread per connection server connection handler for incoming requests.
  We separate the reader and the writer thread, though this isn't actually
  necessary.
 */
public class InternalTCPReceiver {
    private static Logger logger = LoggerFactory.getLogger(InternalTCPReceiver.class);

    public InternalTCPReceiver(final int port, final RequestDispatcher dispatcher) throws IOException {
        final ServerSocketChannel serverSocket = ServerSocketChannel.open();
        serverSocket.socket().bind(new InetSocketAddress(port));

        logger.info("Listening to internal connections on "+serverSocket);

        new Thread(new InternalIncomingServer(serverSocket, dispatcher)).start();
    }

    private class InternalIncomingServer implements Runnable {
        ServerSocketChannel serverSocket;
        RequestDispatcher dispatcher;

        private InternalIncomingServer(ServerSocketChannel serverSocket, RequestDispatcher dispatcher) {
            this.serverSocket = serverSocket;
            this.dispatcher = dispatcher;
        }

        @Override
        public void run() {
            while(true) {
                try {
                    SocketChannel clientSocket = serverSocket.accept();

                    new Thread(new InternalConnectionHandler(clientSocket, dispatcher)).start();
                } catch(IOException e) {
                    logger.warn("Error accepting socket on "+serverSocket, e);
                }
            }
        }
    }

    private class InternalConnectionHandler implements Runnable {
        RequestDispatcher dispatcher;
        KryoSerializer serializer = new KryoSerializer();
        SocketChannel clientSocket;

        public InternalConnectionHandler(SocketChannel clientSocket, RequestDispatcher dispatcher) {
            this.dispatcher = dispatcher;
            this.clientSocket = clientSocket;

            serializer.setInputStream(Channels.newInputStream(clientSocket));
        }

        @Override
        public void run() {
            try {
                while(true) {
                    Object toRead = serializer.getObject();
                    dispatcher.processInbound((KaijuMessage) toRead);
                }
            } catch (KryoException e) {
                logger.error("Error reading from client "+clientSocket, e);
                try {
                    clientSocket.close();
                } catch(IOException ex) {
                    logger.error("Exception closing connection: ", ex);
                }
            }
        }
    }
}
