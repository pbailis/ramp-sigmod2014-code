package edu.berkeley.kaiju.data;

import edu.berkeley.kaiju.util.Timestamp;
import org.apache.hadoop.util.bloom.BloomFilter;

import java.util.Collection;

/*
 Since we implemented so many different algorithms for the paper, this
 class bloated quite a bit. Annotations inline.
 */
public class DataItem {
    // Every item has a version
    private long timestamp = Timestamp.NO_TIMESTAMP;
    private byte[] value;

    // Used in RAMP-Fast
    private Collection<String> transactionKeys = null;

    // Used in RAMP-Hybrid
    private BloomFilter bloomTransactionKeys = null;

    public DataItem(long timestamp, byte[] value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public DataItem(long timestamp, byte[] value, Collection<String> transactionKeys) {
        this(timestamp, value);
        this.transactionKeys = transactionKeys;
    }

    public DataItem(long timestamp, byte[] value, BloomFilter bloomTransactionKeys) {
        this(timestamp, value);
        this.bloomTransactionKeys = bloomTransactionKeys;
    }

    private DataItem() {}

    public long getTimestamp() {
        return timestamp;
    }

    public boolean hasTransactionKeys() {
        return transactionKeys != null;
    }

    public byte[] getValue() {
        return value;
    }

    public Collection<String> getTransactionKeys() {
        return transactionKeys;
    }

    public BloomFilter getBloomTransactionKeys() {
        return bloomTransactionKeys;
    }

    public static DataItem getNullItem() {
        return new DataItem(Timestamp.NO_TIMESTAMP, new byte[0]);
    }
}