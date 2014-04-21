package edu.berkeley.kaiju.service.request.message.response;

import edu.berkeley.kaiju.service.request.message.KaijuMessage;

public class EigerPreparedResponse extends KaijuMessage {
    public long transactionID;
    public int numKeys;
    public long preparedTime;

    private EigerPreparedResponse() {}

    public EigerPreparedResponse(long transactionID, int numKeys, long preparedTime) {
        this.transactionID = transactionID;
        this.numKeys = numKeys;
        this.preparedTime = preparedTime;
    }
}