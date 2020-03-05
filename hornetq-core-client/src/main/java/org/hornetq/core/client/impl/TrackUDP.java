/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hornetq.core.client.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.hornetq.api.core.UDPBroadcastGroupConfiguration;
import org.hornetq.core.cluster.DiscoveryEntry;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.cluster.DiscoveryListener;

public class TrackUDP {

   static SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

   static public void log (String info) {
      synchronized (formatter) {
         System.out.println(formatter.format(new Date()) + " " + info);
      }
   }

   static class TimestamListeners implements DiscoveryListener  {

      final boolean print;
      final int id;
      volatile boolean suspecting = false;

      TimestamListeners(int id, boolean print) {
         this.id = id;
         this.print = print;
      }


      volatile long lastTime = System.currentTimeMillis();

      @Override
      public void connectorsChanged(List<DiscoveryEntry> newConnectors) {
         if (print) {
            log("Listener " + id + " had seen a connector change, current list size :: " + newConnectors.size());
         }
      }

      @Override
      public void udpReceived() {
         if (suspecting) {
            suspecting = false;
            log("Receiving data after some time of inactivity:: " + (System.currentTimeMillis() - lastTime));
         }
         lastTime = System.currentTimeMillis();
      }


   }

   public static void main(String arg[]) {

      System.out.print("Arguments:: ");

      for (String a : arg) {
         System.out.print(a + " ");
      }
      System.out.println();

      if (arg.length < 7) {
         System.out.println("Use: ./run.sh group-ip group-port passive-threads active-threads timeout sleep retries script...");
         System.out.println("");
         System.out.println("retries:: 0 or <0 means retry forever");
         System.out.println("example: ./run.sh 231.7.7.7 9876 20 20 10000 0 10 bash netstat -nu");
         System.exit(-1);
      }


      String group = arg[0];
      int port = Integer.parseInt(arg[1]);
      int passiveThreads = Integer.parseInt(arg[2]);
      int activeThreads = Integer.parseInt(arg[3]);
      int timeout = Integer.parseInt(arg[4]);
      int sleep = Integer.parseInt(arg[5]);
      int retries = Integer.parseInt(arg[6]);

      String script[] = null;
      if (arg.length > 7) {
         script = new String[arg.length - 7];
         int pos = 0;
         for (int i = 7; i < arg.length; i++) {
            script[pos++] = arg[i];
         }

         for (String str : script) {
            System.out.println("Script:: " + str);
         }
      }


      log("Group :: " + group + " port :: " + port + " threads :: " + passiveThreads + " timeout ::" + timeout + " sleep ::" + sleep + " retries :: " + retries + " script:: " + script);

      DiscoveryGroup[] discoveryGroups = new DiscoveryGroup[passiveThreads];

      TimestamListeners[] timestamListeners = new TimestamListeners[passiveThreads];

      try {
         for (int i = 0; i < passiveThreads; i++) {
            UDPBroadcastGroupConfiguration udpBroadcastGroupConfiguration = new UDPBroadcastGroupConfiguration(arg[0], port, null, -1);
            discoveryGroups[i] = new DiscoveryGroup(UUID.randomUUID().toString(), "test" + i, timeout, udpBroadcastGroupConfiguration.createBroadcastEndpointFactory(), null);
            timestamListeners[i] = new TimestamListeners(i, true);
            discoveryGroups[i].registerListener(timestamListeners[i]);
            discoveryGroups[i].start();
         }
      } catch (Exception e) {
         e.printStackTrace();
         System.exit(-1);
      }

      for (int i = 0; i < activeThreads; i++) {
         ActiveConnectionThread activeConnectionThread = new ActiveConnectionThread(group, port, timeout, sleep, retries, script, i);
         activeConnectionThread.start();
      }

      while (true) {
         try {

            checkListeners(timeout, timestamListeners);
            Thread.sleep(1000);
         } catch (Exception e) {
            e.printStackTrace();
         }
      }
   }


   static class ActiveConnectionThread extends Thread {
      private final String group;
      private final int port;
      private final int timeout;
      private final int sleep;
      private int retries;
      private final String[] script;
      private final int runnerId;

      public ActiveConnectionThread(String group, int port, int timeout, int sleep, int retries, String[] script, int id) {
         super("ActiveConnection::" + id);
         this.group = group;
         this.port = port;
         this.timeout = timeout;
         this.sleep = sleep;
         this.script = script;
         this.retries = retries;
         this.runnerId = id;
      }

      @Override
      public void run() {

         while (true) {
            DiscoveryGroup newGroup = null;
            try {
               if (sleep > 0) {
                  Thread.sleep(sleep);
               }

               UDPBroadcastGroupConfiguration udpBroadcastGroupConfiguration = new UDPBroadcastGroupConfiguration(group, port, null, -1);
               newGroup = new DiscoveryGroup(UUID.randomUUID().toString(), "retry-discovery", 30000l, udpBroadcastGroupConfiguration.createBroadcastEndpointFactory(), null);
               newGroup.registerListener(new TimestamListeners(1000, false));
               newGroup.start(); // opening the UDP connection, and starting the receiving thread
               long retryNR = 0;
               while (true) {

                  retryNR++;

                  if (newGroup.waitForBroadcast(timeout)) { // This will wait the read and notification
                     break;
                  } else {
                     log(Thread.currentThread().getName() + "::WARNING! Error Condition! UDP connection did not receive any data, retry " + retryNR + " of " + (retries > 0 ? "" + retries : "INFINITE") + " on runnerID=" + runnerId);
                     if (retryNR == 1 && script != null) {
                        callScript(script, runnerId, retryNR);
                     }
                  }

                  if (retryNR > 0 && retryNR >= retries) {
                     log(Thread.currentThread().getName() + "::Giving up retry loop, trying a new connection now");
                     break;
                  }
               }
            } catch (Exception e) {
               e.printStackTrace();
            } finally {
               newGroup.stop();
            }

         }
      }
   }

   private static void checkListeners(int timeout, TimestamListeners[] timestamListeners) {
      for (TimestamListeners listener : timestamListeners) {
         long timePassed = System.currentTimeMillis() - listener.lastTime;
         if (timePassed > timeout) {
            listener.suspecting = true;
            log("Listener " + listener.id + " did not receive a packet for " + timePassed + " milliseconds");
         }
      }
   }

   private static void callScript(String[] script, long id, long retryNR) {

      if (script != null) {
         try {
            ProcessBuilder builder = new ProcessBuilder(script);
            builder.command().add("" + id);
            builder.command().add("" + retryNR);
            Process process = builder.start();
            ProcessLogger logger = new ProcessLogger(true, process.getErrorStream(), "script err");
            logger.start();
            ProcessLogger loggerOut = new ProcessLogger(true, process.getInputStream(), "script out");
            loggerOut.start();
         } catch (Exception e) {
            e.printStackTrace(System.out);
         }
      }
   }



   /**
    * Redirect the input stream to a logger (as debug logs)
    */
   static class ProcessLogger extends Thread
   {
      private final InputStream is;

      private final String className;

      private final boolean print;

      ProcessLogger(final boolean print, final InputStream is, final String className) throws ClassNotFoundException
      {
         this.is = is;
         this.print = print;
         this.className = className;
         setDaemon(true);
      }

      @Override
      public void run()
      {
         try
         {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            while ((line = br.readLine()) != null)
            {
               if (print)
               {
                  log(className + ":" + line);
               }
            }
         }
         catch (IOException ioe)
         {
            ioe.printStackTrace();
         }
      }
   }
}
