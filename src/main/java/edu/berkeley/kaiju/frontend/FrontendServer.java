package edu.berkeley.kaiju.frontend;

import com.esotericsoftware.kryo.KryoException;
import edu.berkeley.kaiju.frontend.request.ClientRequest;
import edu.berkeley.kaiju.frontend.response.ClientError;
import edu.berkeley.kaiju.frontend.response.ClientResponse;
import edu.berkeley.kaiju.service.request.handler.KaijuServiceHandler;
import edu.berkeley.kaiju.util.KryoSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class FrontendServer {
    private static Logger logger = LoggerFactory.getLogger(FrontendServer.class);
    private ServerSocketChannel serverSocket;
    private KaijuServiceHandler handler;

    public FrontendServer(KaijuServiceHandler handler, int port) throws IOException {
        this.serverSocket = ServerSocketChannel.open();
        serverSocket.socket().bind(new InetSocketAddress(port));
        this.handler = handler;
    }

    public void serve() {
        logger.info("Listening to external connections on "+serverSocket);

        (new FrontendConnectionServer(serverSocket, handler)).run();
    }

    private class FrontendConnectionServer implements Runnable {
        ServerSocketChannel serverSocket;
        KaijuServiceHandler handler;

        private FrontendConnectionServer(ServerSocketChannel serverSocket, KaijuServiceHandler handler) {
            this.serverSocket = serverSocket;
            this.handler = handler;
        }

        @Override
        public void run() {
            while(true) {
                try {
                    SocketChannel clientSocket = serverSocket.accept();

                    new Thread(new FrontendConnectionHandler(clientSocket, handler)).start();
                } catch(IOException e) {
                    logger.warn("Error accepting socket on "+serverSocket, e);
                }
            }
        }
    }
    private class FrontendConnectionHandler implements Runnable {
        KryoSerializer serializer = new KryoSerializer();
        KaijuServiceHandler handler = null;
        SocketChannel clientSocket = null;

        public FrontendConnectionHandler(SocketChannel clientSocket, KaijuServiceHandler handler) {
            this.clientSocket = clientSocket;
            serializer.setInputStream(Channels.newInputStream(clientSocket));
            serializer.setOutputStream(Channels.newOutputStream(clientSocket));
            this.handler = handler;
        }

        @Override
        public void run() {
            try {
                while(true) {
                    Object request = serializer.getObject();

                    if(request instanceof String) {
                        if(request.equals("EXIT")) {
                            clientSocket.close();
                            return;
                        }
                    }

                    ClientResponse response = handler.processRequest((ClientRequest) request);
                    serializer.serialize(response);
                }
            } catch (KryoException e) {
                logger.error("Kryo error with client "+clientSocket, e);

                try {
                    clientSocket.close();
                } catch(IOException ex) {
                    logger.error("Exception closing connection: ", ex);
                }
            } catch (Exception e) {
                logger.error("Error executing request for client "+clientSocket, e);
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);

                try {
                    serializer.serialize(new ClientError(e+" "+sw.toString()));
                } catch (IOException ioe) {
                    logger.error("Error executing serialize for client error", ioe);
                }

            }
        }
    }
}
