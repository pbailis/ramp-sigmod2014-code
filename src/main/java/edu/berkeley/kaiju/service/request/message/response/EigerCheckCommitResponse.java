package edu.berkeley.kaiju.service.request.message.response;

import java.util.Map;

public class EigerCheckCommitResponse extends KaijuResponse {
    public Map<Long, Long> commitTimes;

    private EigerCheckCommitResponse() {}

    public EigerCheckCommitResponse(Map<Long, Long> commitTimes) {
        this.commitTimes = commitTimes;
    }
}