package edu.berkeley.kaiju.data;

/*
 Utility class for identifying a key-timestamp pair.
 In retrospect, not sure why we didn't override HashCode or equals().
 */
public class ItemVersion {
    private String key;
    private long timestamp;
    byte[] keyBytes = null;

    private ItemVersion() {}

    public ItemVersion(String key, long timestamp) {
        this.timestamp = timestamp;
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public byte[] getKeyBytes() {
        if(keyBytes == null) {
            keyBytes = key.getBytes();
        }

        return keyBytes;
    }
}