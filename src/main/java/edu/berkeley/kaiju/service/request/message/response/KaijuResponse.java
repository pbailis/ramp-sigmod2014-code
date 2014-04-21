package edu.berkeley.kaiju.service.request.message.response;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import edu.berkeley.kaiju.data.DataItem;
import edu.berkeley.kaiju.exception.RemoteOperationException;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class KaijuResponse extends KaijuMessage {
    // used in GET_ALL, GETALL_BY_TIMESTAMP
    public Map<String, DataItem> keyValuePairs;

    // used in GET_TIMESTAMPS_REQUEST
    public Collection<Long> timestamps;

    // used in GET_MESSAGE
    public DataItem dataItem;

    // used in CHECK_PREPARED
    public Boolean prepared;

    public List<String> errors;

    public KaijuResponse(Map<String, DataItem> keyValuePairs) {
        this.keyValuePairs = keyValuePairs;
    }

    public KaijuResponse(Collection<Long> timestamps) {
        this.timestamps = timestamps;
    }

    public KaijuResponse(DataItem dataItem) {
        this.dataItem = dataItem;
    }

    public KaijuResponse(List<String> errors) {
        this.errors = errors;
    }

    public KaijuResponse(boolean prepared) {
        this.prepared = prepared;
    }

    public KaijuResponse() {}

    public boolean hasErrors() {
        return errors != null;
    }

    public String getErrorString() {
        Joiner joiner = Joiner.on(";");
        return joiner.join(errors);
    }

    public static void coalesceErrorsIntoException(Collection<KaijuResponse> responses) throws
                                                                                        RemoteOperationException {
        Collection<String> errors = null;
        for(KaijuResponse response : responses) {
            if(response.hasErrors()) {
                if(errors == null)
                    errors = Lists.newArrayList();
                errors.add(response.senderID + ": " + response.getErrorString());
            }
        }

        if(errors != null)
            throw new RemoteOperationException(Joiner.on("; ").join(errors));
    }
}
