
package edu.berkeley.kaiju.cmdline;

import edu.berkeley.kaiju.frontend.KaijuClient;

import java.lang.Exception;
import java.lang.Long;
import java.lang.String;
import java.lang.System;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.lang.Thread;
import java.nio.ByteBuffer;

public class CommandLineClient {
    private static enum MODE { PUT_ALL, GET_ALL }

    public static void main(String[] args) {
        try {
            KaijuClient client = new KaijuClient("127.0.0.1", 8080);

            MODE mode = MODE.PUT_ALL;

            if(args.length > 0) {
                if(args[0].equalsIgnoreCase("PA")) {
                    mode = MODE.PUT_ALL;
                }
                else if(args[0].equalsIgnoreCase("GA")) {
                    mode = MODE.GET_ALL;
                }
            }

            if(mode == MODE.PUT_ALL)
                System.out.println("PUTALL");
            else if(mode == MODE.GET_ALL)
                System.out.println("GETALL");

            System.out.println("Hello, World");

            long counter = 0;
            long startTime = System.currentTimeMillis();
            while(true) {
                counter++;
                if(counter % 1000 == 0) {
                    System.out.println(counter/(System.currentTimeMillis()-startTime+1.)*1000.);
                }

                if(mode == MODE.PUT_ALL) {
                    Map toPut = new HashMap<String, ByteBuffer>();
                    toPut.put("a", String.format("%d", counter).getBytes());
                    toPut.put("b", String.format("%d", counter).getBytes());
                    client.put_all(toPut);
                } else if(mode == MODE.GET_ALL) {
                    ArrayList<String> toGet = new ArrayList<String>();
                    toGet.add("a");
                    toGet.add("b");
                    Map<String, byte[]> ret = client.get_all(toGet);
                    if(!Arrays.equals(ret.get("a"), ret.get("b"))) {
                        System.err.println("ERROR: 'a' was "+Arrays.toString(ret.get("a"))
                                           + " but 'b' was "+Arrays.toString(ret.get("b")));
                    }
                }
            }
        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
        }
    }

}
