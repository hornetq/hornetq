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

package org.hornetq.core.server.cluster.impl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.api.core.HornetQIllegalStateException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.UDPBroadcastGroupConfiguration;
import org.hornetq.core.cluster.DiscoveryEntry;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.cluster.DiscoveryListener;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.server.NodeManager;
import org.hornetq.utils.UUIDGenerator;

public class TrackUDPServer {

   static SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

   static public void log (String info) {
      synchronized (formatter) {
         System.out.println(formatter.format(new Date()) + " " + info);
      }
   }
   public static void main(String arg[]) {

      System.out.print("Arguments:: ");

      for (String a : arg) {
         System.out.print(a + " ");
      }
      System.out.println();

      if (arg.length != 5) {
         log("Use: ./server.sh group-ip group-port InformationIP InformationPort period");
         log("example: ./server.sh 231.7.7.7 9876 10.0.0.1 8088 1000");
         System.exit(-1);
      }


      String group = arg[0];
      int port = Integer.parseInt(arg[1]);
      String informationIP = arg[2];
      int informationPort= Integer.parseInt(arg[3]);
      int period = Integer.parseInt(arg[4]);

      if (period <= 0) {
         System.err.println("period has to be > 0");
         System.exit(-1);
      }

      log("Group :: " + group + " port :: " + port + " informationIP:: " + informationIP + " informationPort:: " + informationPort + " sleep:: " + period);

      try {
         DiscoveryGroup newGroup = null;
         UDPBroadcastGroupConfiguration udpBroadcastGroupConfiguration = new UDPBroadcastGroupConfiguration(arg[0], port, null, -1);
         NodeManager nodeManager = new FakeNodeManager(false, ".");
         nodeManager.setNodeID(UUIDGenerator.getInstance().generateStringUUID());

         ScheduledExecutorService executorService = Executors.newScheduledThreadPool(5);

         BroadcastGroupImpl broadcastGroup = new BroadcastGroupImpl(nodeManager, "server", period, executorService, udpBroadcastGroupConfiguration.createBroadcastEndpointFactory());

         TransportConfiguration transportConfiguration = new TransportConfiguration(NettyConnectorFactory.class.getName());
         transportConfiguration.getParams().put("host", informationIP);
         transportConfiguration.getParams().put("port", informationPort);
         broadcastGroup.addConnector(transportConfiguration);

         broadcastGroup.start();


         while (true) {
            Thread.sleep(10000); // loop forever
         }
      } catch (Exception e) {
         e.printStackTrace();
         System.exit(-1);
      }
   }


   public static class FakeNodeManager extends NodeManager {

      public FakeNodeManager(boolean replicatedBackup, String directory) {
         super(replicatedBackup, directory);
      }

      @Override
      public void awaitLiveNode() throws Exception {

      }

      @Override
      public void startBackup() throws Exception {

      }

      @Override
      public void startLiveNode() throws Exception {

      }

      @Override
      public void pauseLiveServer() throws Exception {

      }

      @Override
      public void crashLiveServer() throws Exception {

      }

      @Override
      public void releaseBackup() throws Exception {

      }

      @Override
      public SimpleString readNodeId() throws HornetQIllegalStateException, IOException {
         return null;
      }

      @Override
      public boolean isAwaitingFailback() throws Exception {
         return false;
      }

      @Override
      public boolean isBackupLive() throws Exception {
         return false;
      }

      @Override
      public void interrupt() {

      }
   }

}
