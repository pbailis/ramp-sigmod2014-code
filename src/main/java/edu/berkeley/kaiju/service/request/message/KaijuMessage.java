package edu.berkeley.kaiju.service.request.message;

import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;

/*
 To allow multiple outstanding RPCs on a given socket, each RPC message is assigned
 a requestID (by the sender; see util.Timestamp) and a sender ID (for dispatch).
 */
public abstract class KaijuMessage {
    public int requestID;
    public short senderID;

    public boolean isResponse() {
        return this instanceof KaijuResponse;
    }
}