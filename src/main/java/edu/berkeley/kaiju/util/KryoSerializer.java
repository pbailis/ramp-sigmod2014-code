package edu.berkeley.kaiju.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.berkeley.kaiju.config.Config;
import edu.berkeley.kaiju.data.DataItem;
import edu.berkeley.kaiju.data.ItemVersion;
import edu.berkeley.kaiju.exception.AbortedException;
import edu.berkeley.kaiju.frontend.request.ClientGetAllRequest;
import edu.berkeley.kaiju.frontend.request.ClientPutAllRequest;
import edu.berkeley.kaiju.frontend.request.ClientRequest;
import edu.berkeley.kaiju.frontend.request.ClientSetIsolationRequest;
import edu.berkeley.kaiju.frontend.response.*;
import edu.berkeley.kaiju.service.LockManager.LockDuration;
import edu.berkeley.kaiju.service.request.message.KaijuMessage;
import edu.berkeley.kaiju.service.request.message.request.*;
import edu.berkeley.kaiju.service.request.message.response.EigerCheckCommitResponse;
import edu.berkeley.kaiju.service.request.message.response.EigerPreparedResponse;
import edu.berkeley.kaiju.service.request.message.response.KaijuResponse;
import org.apache.hadoop.util.bloom.BloomFilter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class KryoSerializer {
    private Output output;
    private Input input;
    private Kryo kryo;

    public KryoSerializer() {
        kryo = new Kryo();
        kryo.register(CommitPutAllRequest.class);
        kryo.register(GetAllByTimestampListRequest.class);
        kryo.register(GetAllByTimestampRequest.class);
        kryo.register(GetAllRequest.class);
        kryo.register(GetRequest.class);
        kryo.register(GetTimestampsRequest.class);
        kryo.register(KaijuMessage.class);
        kryo.register(KaijuResponse.class);
        kryo.register(IKaijuRequest.class);
        kryo.register(PreparePutAllRequest.class);
        kryo.register(PutAllRequest.class);
        kryo.register(PutRequest.class);
        kryo.register(ReadLockRequest.class);
        kryo.register(UnlockRequest.class);
        kryo.register(WriteLockRequest.class);
        kryo.register(DataItem.class);
        kryo.register(ItemVersion.class);
        kryo.register(byte[].class);
        kryo.register(ArrayList.class);
        kryo.register(HashMap.class);
        kryo.register(HashSet.class);
        kryo.register(LockDuration.class);
        kryo.register(EigerCheckCommitRequest.class);
        kryo.register(EigerCommitRequest.class);
        kryo.register(EigerGetAllRequest.class);
        kryo.register(EigerPutAllRequest.class);
        kryo.register(EigerCheckCommitRequest.class);
        kryo.register(EigerPreparedResponse.class);
        kryo.register(EigerCheckCommitResponse.class);
        kryo.register(BloomFilter.class);
        kryo.register(GetEachByTimestampListRequest.class);
        kryo.register(ClientGetAllRequest.class);
        kryo.register(ClientPutAllRequest.class);
        kryo.register(ClientRequest.class);
        kryo.register(ClientError.class);
        kryo.register(ClientGetAllRequest.class);
        kryo.register(ClientPutAllRequest.class);
        kryo.register(ClientResponse.class);
        kryo.register(ClientPutAllResponse.class);
        kryo.register(ClientGetAllResponse.class);
        kryo.register(ClientSetIsolationRequest.class);
        kryo.register(ClientSetIsolationResponse.class);
        kryo.register(Config.IsolationLevel.class);
        kryo.register(Config.ReadAtomicAlgorithm.class);
        kryo.register(AbortedException.class);
        kryo.register(CheckPreparedRequest.class);

        kryo.setRegistrationRequired(true);
    }

    public void setInputStream(InputStream inputStream) {
        this.input = new Input(inputStream, Config.getConfig().max_object_size);
    }

    public void setOutputStream(OutputStream outputStream) {
        this.output = new Output(outputStream, Config.getConfig().max_object_size);
    }

    public void serialize(Object object) throws IOException {
        kryo.writeClassAndObject(this.output, object);
        this.output.flush();
        this.output.getOutputStream().flush();
    }

    public Object getObject() {
        return kryo.readClassAndObject(input);
    }
}