package edu.berkeley.kaiju.net.callback;

import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;

import java.util.Collection;
import java.util.Vector;
import java.util.concurrent.Semaphore;

/*
 In the future, I'd probably use futures here.
 */
public class MultiMessageCallback implements IMessageCallback {
    private Vector<KaijuResponse> responses = new Vector<KaijuResponse>();
    private Semaphore responseSemaphore;
    private int numMessages;

    public MultiMessageCallback(int numMessages) {
        responseSemaphore = new Semaphore(0);
        this.numMessages = numMessages;
    }

    // TODO: add method for a timed block
    public Collection<KaijuResponse> blockForResponses() throws InterruptedException {
        responseSemaphore.acquireUninterruptibly(numMessages);

        return responses;
    }

    public void notifyResponse(KaijuMessage response) {
        responses.add((KaijuResponse) response);
        responseSemaphore.release();
    }
}