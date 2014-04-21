package edu.berkeley.kaiju.service.request.message.request;

import edu.berkeley.kaiju.service.request.message.KaijuMessage;

public class EigerCommitRequest extends KaijuMessage {
    public long transactionID;
    public long commitTime;

    private EigerCommitRequest() {}

    public EigerCommitRequest(long transactionID, long commitTime) {
        this.transactionID = transactionID;
        this.commitTime = commitTime;
    }
}