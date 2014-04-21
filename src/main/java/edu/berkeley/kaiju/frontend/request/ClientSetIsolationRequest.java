package edu.berkeley.kaiju.frontend.request;

import edu.berkeley.kaiju.config.Config;

public class ClientSetIsolationRequest extends ClientRequest {
    public Config.IsolationLevel isolationLevel;
    public Config.ReadAtomicAlgorithm readAtomicAlgorithm;

    ClientSetIsolationRequest() {};

    public ClientSetIsolationRequest(Config.IsolationLevel isolationLevel, Config.ReadAtomicAlgorithm readAtomicAlgorithm) {
        this.isolationLevel = isolationLevel;
        this.readAtomicAlgorithm = readAtomicAlgorithm;
    }
}
