package edu.berkeley.kaiju.net.callback;

import edu.berkeley.kaiju.service.request.message.KaijuMessage;

public interface IMessageCallback {
    public void notifyResponse(KaijuMessage response);
}