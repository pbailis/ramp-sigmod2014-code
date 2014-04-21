/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import edu.berkeley.kaiju.config.Config;

import java.lang.Exception;
import java.lang.Integer;
import java.lang.Math;
import java.lang.String;
import java.lang.System;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import java.util.Random;
import java.util.Properties;
import java.nio.ByteBuffer;

public class KaijuClient extends DB
{

    public static final int Ok = 0;
    public static final int Error = -1;

    boolean terminated = false;

    Map<String, byte[]> toPut = new HashMap<String, byte[]>();
    List<String> toGet = new ArrayList<String>();

    @Override
    public void preLoadBootstrap() {
        String hosts = getProperties().getProperty("hosts");
        if (hosts == null)
        {
            throw new RuntimeException("Required property \"hosts\" missing for KaijuClient");
        }

        List<Thread> configThreads = new ArrayList<Thread>();
        List<ChangeIsolationRequest> isolationRequests = new ArrayList<ChangeIsolationRequest>();

        try {
            String[] hostsarr = hosts.split(",");
            Config.IsolationLevel isolationLevel = Config.IsolationLevel.valueOf(getProperties().getProperty("isolation_level"));
            Config.ReadAtomicAlgorithm readAtomicAlgorithm = Config.ReadAtomicAlgorithm.valueOf(getProperties().getProperty("read_atomic_algorithm"))   ;

            for(String serverStr : hostsarr) {
                String serverHost = serverStr.split(":")[0];
                int serverPort = Integer.parseInt(serverStr.split(":")[1]);

                ChangeIsolationRequest request = new ChangeIsolationRequest(isolationLevel, readAtomicAlgorithm, serverHost, serverPort);
                Thread changeIsolationThread = new Thread(request);

                isolationRequests.add(request);
                changeIsolationThread.start();
                configThreads.add(changeIsolationThread);
            }

            for(Thread requestThread : configThreads) {
                requestThread.join();
            }

            boolean hasError = false;

            for(ChangeIsolationRequest request : isolationRequests) {
                if(request.hasError()) {
                    hasError = true;
                    System.err.println("Error setting isolation"+request.getError());
                }
            }

            if(hasError) {
                throw new RuntimeException("Error setting isolation; cowardly failing");
            }

            System.out.println("Set isolation!");

        } catch (Exception e) {
            throw new RuntimeException("Error setting isolation!", e);
        }
    }

    private class ChangeIsolationRequest implements Runnable {
        Config.IsolationLevel isolationLevel;
        Config.ReadAtomicAlgorithm readAtomicAlgorithm;
        String host;
        int port;

        Exception error;

        private ChangeIsolationRequest(Config.IsolationLevel isolationLevel,
                                       Config.ReadAtomicAlgorithm readAtomicAlgorithm,
                                       String host,
                                       int port) {
            this.isolationLevel = isolationLevel;
            this.readAtomicAlgorithm = readAtomicAlgorithm;
            this.host = host;
            this.port = port;
        }

        public void run() {
            try {
                edu.berkeley.kaiju.frontend.KaijuClient client = new edu.berkeley.kaiju.frontend.KaijuClient(host, port);
                client.setIsolation(isolationLevel, readAtomicAlgorithm);
                client.close();
            } catch (Exception e) {
                error = e;
            }
        }

        public boolean hasError() {
            return error != null;
        }

        public Exception getError() {
            return error;
        }
    }

    edu.berkeley.kaiju.frontend.KaijuClient client;
    /**
     * Initialize any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    public void init() throws DBException
    {
        String hosts = getProperties().getProperty("hosts");
        if (hosts == null)
        {
            throw new DBException("Required property \"hosts\" missing for KaijuClient");
        }


        String serverStr = "unassigned";

        String[] hostsarr = hosts.split(",");

        serverStr = hostsarr[Math.abs(new Random().nextInt()) % hostsarr.length];

        String serverHost = serverStr.split(":")[0];
        int serverPort = Integer.parseInt(serverStr.split(":")[1]);


        try {
            try {
                Thread.sleep(new Random().nextInt(10000));
            } catch (InterruptedException e) {

            }

            client = new edu.berkeley.kaiju.frontend.KaijuClient(serverHost, serverPort);
        } catch (Exception e) {
            System.err.println("Error for host "+serverHost+" "+serverPort);
	    e.printStackTrace();
            throw new DBException(e);
        }
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one DB
     * instance per client thread.
     */
    public void cleanup() throws DBException
    {
        try {
            terminated = true;
            client.close();
        } catch (Exception e) {
            throw new DBException(e);
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result will
     * be stored in a HashMap.
     *
     * @param table
     *          The name of the table
     * @param key
     *          The record key of the record to read.
     * @param fields
     *          The list of fields to read, or null for all of them
     * @param result
     *          A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result)
    {
        try {
            toGet.add(key);
        } catch (Exception e) {
            System.err.println(e);
	    e.printStackTrace();
            return Error;
        }
        return Ok;
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value
     * pair from the result will be stored in a HashMap.
     *
     * @param table
     *          The name of the table
     * @param startkey
     *          The record key of the first record to read.
     * @param recordcount
     *          The number of records to read
     * @param fields
     *          The list of fields to read, or null for all of them
     * @param result
     *          A Vector of HashMaps, where each HashMap is a set field/value
     *          pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    public int scan(String table, String startkey, int recordcount, Set<String> fields,
                    Vector<HashMap<String, ByteIterator>> result)
    {
        return Error;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     *
     * @param table
     *          The name of the table
     * @param key
     *          The record key of the record to write.
     * @param values
     *          A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int update(String table, String key, HashMap<String, ByteIterator> values)
    {
        return insert(table, key, values);
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key.
     *
     * @param table
     *          The name of the table
     * @param key
     *          The record key of the record to insert.
     * @param values
     *          A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int insert(String table, String key, HashMap<String, ByteIterator> values)
    {
        try {
            toPut.put(key, values.values().iterator().next().toArray());
        } catch (Exception e) {
            System.err.println(e);
            return Error;
        }
        return Ok;
    }

    /**
     * Delete a record from the database.
     *
     * @param table
     *          The name of the table
     * @param key
     *          The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    public int delete(String table, String key)
    {
        try {
            toPut.put(key, new byte[0]);
        } catch (Exception e) {
            System.err.println(e);
	    e.printStackTrace();
            return Error;
        }

        return Ok;
    }

    public int numOperationsQueued() {
        return Math.max(toPut.size(), toGet.size());
    }

    public int commit() {

        if(terminated)
            return Error;

        try {
            if(!toPut.isEmpty()) {
                try {
                    client.put_all(toPut);
                    if(toGet.size() != 0) {
                        System.err.println("Whoa? Get-only and put only!");
                    }
                } catch (Exception e) {
                    System.err.println(e);
                    e.printStackTrace();
                    return Error;
                }
            }

            if(!toGet.isEmpty()) {
                try {
                      client.get_all(toGet);

                      if(toPut.size() != 0) {
                          System.err.println("Whoa? Get-only and put only!");
                      }
                  } catch (Exception e) {
                      System.err.println(e);
                      e.printStackTrace();
                      return Error;
                }
            }

            return Ok;
        } finally {
            toPut.clear();
            toGet.clear();
        }
    }

    public static void main(String[] args)
    {

    }
}
