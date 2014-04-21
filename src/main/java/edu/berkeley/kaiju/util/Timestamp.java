package edu.berkeley.kaiju.util;

import edu.berkeley.kaiju.config.Config;

public class Timestamp {
    /*
      Timestamps are comprised of:
        38 bits for realtime, measured in ms  -> 8.7 year wraparound
        14 bits for sequence no within a ms -> max 16,384,000 operations/sec
        12 bits for serverid
     */

    public final static long NO_TIMESTAMP = -1;

    private static volatile long latestMillis = -1;
    private static volatile int sequenceNo = 0;

    public static long assignNewTimestamp() {
        return assignNewTimestamp(-1);
    }

    public static synchronized long assignNewTimestamp(long min) {
        long chosenTime = Math.max(System.currentTimeMillis(), min >> 26);
        int chosenSeqNo = 0;

        if (latestMillis < chosenTime) {
            latestMillis = chosenTime;
            sequenceNo = chosenSeqNo = 0;
        } else if (latestMillis == chosenTime) {
            sequenceNo++;
            chosenSeqNo = sequenceNo;
        } else {
            chosenTime = latestMillis;
            sequenceNo++;
            chosenSeqNo = sequenceNo;
        }

        assert sequenceNo < 16384;

        return (chosenTime << 26) | (chosenSeqNo << 12) | (Config.getConfig().server_id);
    }
}