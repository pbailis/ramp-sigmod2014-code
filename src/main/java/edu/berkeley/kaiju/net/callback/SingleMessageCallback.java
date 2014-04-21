package edu.berkeley.kaiju.net.callback;

import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;

public class SingleMessageCallback implements IMessageCallback {
    private KaijuMessage response = null;

    // TODO: add method for a timed block
    public KaijuResponse blockForResponse() throws InterruptedException {
        synchronized (this) {
            if(response == null) {
                this.wait();
            }
        }

        return (KaijuResponse) response;
    }

    public void notifyResponse(KaijuMessage response) {
        synchronized (this) {
            this.response = response;
            this.notify();
        }
    }
}