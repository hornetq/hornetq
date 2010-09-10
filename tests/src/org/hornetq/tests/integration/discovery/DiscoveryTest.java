/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.integration.discovery;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.cluster.DiscoveryEntry;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.cluster.DiscoveryListener;
import org.hornetq.core.cluster.impl.DiscoveryGroupImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.cluster.BroadcastGroup;
import org.hornetq.core.server.cluster.impl.BroadcastGroupImpl;
import org.hornetq.core.server.management.Notification;
import org.hornetq.tests.integration.SimpleNotificationService;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.UUIDGenerator;

/**
 * A DiscoveryTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 8 Dec 2008 12:36:26
 * 
 * 
 *
 *
 */
public class DiscoveryTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(DiscoveryTest.class);

   private static final String address1 = getUDPDiscoveryAddress();

   private static final String address2 = getUDPDiscoveryAddress(1);

   private static final String address3 = getUDPDiscoveryAddress(2);

   public void testSimpleBroadcast() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(DiscoveryTest.address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      final String nodeID = RandomUtil.randomString();

      BroadcastGroup bg = new BroadcastGroupImpl(nodeID,
                                                 RandomUtil.randomString(),
                                                 null,
                                                 -1,
                                                 groupAddress,
                                                 groupPort,
                                                 true);

      bg.start();

      TransportConfiguration live1 = generateTC();

      TransportConfiguration backup1 = generateTC();

      Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration, TransportConfiguration>(live1,
                                                                                                                                    backup1);

      bg.addConnectorPair(connectorPair);

      DiscoveryGroup dg = new DiscoveryGroupImpl(RandomUtil.randomString(),
                                                 RandomUtil.randomString(),
                                                 null,
                                                 groupAddress,
                                                 groupPort,
                                                 timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      Assert.assertTrue(ok);

      Map<String, DiscoveryEntry> entryMap = dg.getDiscoveryEntryMap();

      Assert.assertNotNull(entryMap);

      Assert.assertEquals(1, entryMap.size());

      DiscoveryEntry entry = entryMap.get(nodeID);

      Assert.assertNotNull(entry);

      Assert.assertEquals(connectorPair, entry.getConnectorPair());

      bg.stop();

      dg.stop();

   }
   
   public void testSimpleBroadcastSpecificNIC() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(DiscoveryTest.address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      final String nodeID = RandomUtil.randomString();
      
      //We need to choose a real NIC on the local machine - note this will silently pass if the machine
      //has no usable NIC!
      
      Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
      
      InetAddress localAddress = null;
      
      outer: while (networkInterfaces.hasMoreElements())
      {
         NetworkInterface networkInterface = networkInterfaces.nextElement();
         if (networkInterface.isLoopback() || networkInterface.isVirtual() || !networkInterface.isUp() ||
                  !networkInterface.supportsMulticast())
         {
            continue;
         }
         
         Enumeration<InetAddress> en = networkInterface.getInetAddresses();
         
         while (en.hasMoreElements())
         {
            InetAddress ia = en.nextElement();
            
            if (ia.getAddress().length == 4)
            {
               localAddress = ia;
               
               break outer;
            }
         }
      }
      
      if (localAddress == null)
      {
         log.warn("Can't find address to use");
         
         return;
      }
      
      log.info("Local address is " + localAddress);

      BroadcastGroup bg = new BroadcastGroupImpl(nodeID,
                                                 RandomUtil.randomString(),
                                                 localAddress,
                                                 6552,
                                                 groupAddress,
                                                 groupPort,
                                                 true);

      bg.start();

      TransportConfiguration live1 = generateTC();

      TransportConfiguration backup1 = generateTC();

      Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration, TransportConfiguration>(live1,
                                                                                                                                    backup1);

      bg.addConnectorPair(connectorPair);

      DiscoveryGroup dg = new DiscoveryGroupImpl(RandomUtil.randomString(),
                                                 RandomUtil.randomString(),
                                                 localAddress,
                                                 groupAddress,
                                                 groupPort,
                                                 timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      Assert.assertTrue(ok);

      Map<String, DiscoveryEntry> entryMap = dg.getDiscoveryEntryMap();

      Assert.assertNotNull(entryMap);

      Assert.assertEquals(1, entryMap.size());

      DiscoveryEntry entry = entryMap.get(nodeID);

      Assert.assertNotNull(entry);

      Assert.assertEquals(connectorPair, entry.getConnectorPair());

      bg.stop();

      dg.stop();

   }

   public void testSimpleBroadcastWithStopStartDiscoveryGroup() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(DiscoveryTest.address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      final String nodeID = RandomUtil.randomString();

      BroadcastGroup bg = new BroadcastGroupImpl(nodeID,
                                                 RandomUtil.randomString(),
                                                 null,
                                                 -1,
                                                 groupAddress,
                                                 groupPort,
                                                 true);

      bg.start();

      TransportConfiguration live1 = generateTC();

      TransportConfiguration backup1 = generateTC();

      Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration, TransportConfiguration>(live1,
                                                                                                                                    backup1);

      bg.addConnectorPair(connectorPair);

      DiscoveryGroup dg = new DiscoveryGroupImpl(RandomUtil.randomString(),
                                                 RandomUtil.randomString(),
                                                 null,
                                                 groupAddress,
                                                 groupPort,
                                                 timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      Assert.assertTrue(ok);

      Map<String, DiscoveryEntry> entryMap = dg.getDiscoveryEntryMap();

      Assert.assertNotNull(entryMap);

      Assert.assertEquals(1, entryMap.size());

      DiscoveryEntry entry = entryMap.get(nodeID);

      Assert.assertNotNull(entry);

      Assert.assertEquals(connectorPair, entry.getConnectorPair());

      bg.stop();

      dg.stop();

      dg.start();

      bg.start();

      bg.broadcastConnectors();

      ok = dg.waitForBroadcast(1000);

      Assert.assertTrue(ok);

      entryMap = dg.getDiscoveryEntryMap();

      Assert.assertNotNull(entryMap);

      Assert.assertEquals(1, entryMap.size());

      entry = entryMap.get(nodeID);

      Assert.assertNotNull(entry);

      Assert.assertEquals(connectorPair, entry.getConnectorPair());

   }

   public void testIgnoreTrafficFromOwnNode() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(DiscoveryTest.address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      String nodeID = RandomUtil.randomString();

      BroadcastGroup bg = new BroadcastGroupImpl(nodeID,
                                                 RandomUtil.randomString(),
                                                 null,
                                                 -1,
                                                 groupAddress,
                                                 groupPort,
                                                 true);

      bg.start();

      TransportConfiguration live1 = generateTC();

      TransportConfiguration backup1 = generateTC();

      Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration, TransportConfiguration>(live1,
                                                                                                                                    backup1);

      bg.addConnectorPair(connectorPair);

      DiscoveryGroup dg = new DiscoveryGroupImpl(nodeID, RandomUtil.randomString(), null, groupAddress, groupPort, timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      Assert.assertFalse(ok);

      Map<String, DiscoveryEntry> entryMap = dg.getDiscoveryEntryMap();

      Assert.assertNotNull(entryMap);

      Assert.assertEquals(0, entryMap.size());

      bg.stop();

      dg.stop();

   }

   // There is a bug in some OSes where different addresses but *Same port* will receive the traffic - hence this test
   // won't pass
   // See http://www.jboss.org/community/docs/DOC-11710 (jboss wiki promiscuous traffic)

   // public void testSimpleBroadcastDifferentAddress() throws Exception
   // {
   // final InetAddress groupAddress = InetAddress.getByName(address1);
   // final int groupPort = getUDPDiscoveryPort();
   // final int timeout = 500;
   //
   // BroadcastGroup bg = new BroadcastGroupImpl(randomString(), randomString(), null, -1, groupAddress, groupPort);
   //
   // bg.start();
   //
   // TransportConfiguration live1 = generateTC();
   //
   // TransportConfiguration backup1 = generateTC();
   //
   // Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration,
   // TransportConfiguration>(live1,
   // backup1);
   //
   // bg.addConnectorPair(connectorPair);
   //
   // final InetAddress groupAddress2 = InetAddress.getByName(address2);
   //
   // DiscoveryGroup dg = new DiscoveryGroupImpl(randomString(), randomString(), groupAddress2, groupPort, timeout);
   //
   // dg.start();
   //
   // bg.broadcastConnectors();
   //
   // boolean ok = dg.waitForBroadcast(1000);
   //
   // assertFalse(ok);
   //
   // bg.stop();
   //
   // dg.stop();
   //
   // }

   public void testSimpleBroadcastDifferentPort() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(getUDPDiscoveryAddress());
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      BroadcastGroup bg = new BroadcastGroupImpl(RandomUtil.randomString(),
                                                 RandomUtil.randomString(),
                                                 null,
                                                 -1,
                                                 groupAddress,
                                                 groupPort,
                                                 true);

      bg.start();

      TransportConfiguration live1 = generateTC();

      TransportConfiguration backup1 = generateTC();

      Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration, TransportConfiguration>(live1,
                                                                                                                                    backup1);

      bg.addConnectorPair(connectorPair);

      final int port2 = getUDPDiscoveryPort(1);

      DiscoveryGroup dg = new DiscoveryGroupImpl(RandomUtil.randomString(),
                                                 RandomUtil.randomString(),
                                                 null,
                                                 groupAddress,
                                                 port2,
                                                 timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      Assert.assertFalse(ok);

      bg.stop();

      dg.stop();
   }

   public void testSimpleBroadcastDifferentAddressAndPort() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(DiscoveryTest.address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      BroadcastGroup bg = new BroadcastGroupImpl(RandomUtil.randomString(),
                                                 RandomUtil.randomString(),
                                                 null,
                                                 -1,
                                                 groupAddress,
                                                 groupPort,
                                                 true);

      bg.start();

      TransportConfiguration live1 = generateTC();

      TransportConfiguration backup1 = generateTC();

      Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration, TransportConfiguration>(live1,
                                                                                                                                    backup1);

      bg.addConnectorPair(connectorPair);

      final InetAddress groupAddress2 = InetAddress.getByName(DiscoveryTest.address2);
      final int port2 = getUDPDiscoveryPort(1);

      DiscoveryGroup dg = new DiscoveryGroupImpl(RandomUtil.randomString(),
                                                 RandomUtil.randomString(),
                                                 null,
                                                 groupAddress2,
                                                 port2,
                                                 timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      Assert.assertFalse(ok);

      bg.stop();

      dg.stop();
   }

   public void testMultipleGroups() throws Exception
   {
      final InetAddress groupAddress1 = InetAddress.getByName(DiscoveryTest.address1);
      final int groupPort1 = getUDPDiscoveryPort();

      final InetAddress groupAddress2 = InetAddress.getByName(DiscoveryTest.address2);
      final int groupPort2 = getUDPDiscoveryPort(1);

      final InetAddress groupAddress3 = InetAddress.getByName(DiscoveryTest.address3);
      final int groupPort3 = getUDPDiscoveryPort(2);

      final int timeout = 500;

      String node1 = RandomUtil.randomString();

      String node2 = RandomUtil.randomString();

      String node3 = RandomUtil.randomString();

      BroadcastGroup bg1 = new BroadcastGroupImpl(node1,
                                                  RandomUtil.randomString(),
                                                  null,
                                                  -1,
                                                  groupAddress1,
                                                  groupPort1,
                                                  true);
      bg1.start();

      BroadcastGroup bg2 = new BroadcastGroupImpl(node2,
                                                  RandomUtil.randomString(),
                                                  null,
                                                  -1,
                                                  groupAddress2,
                                                  groupPort2,
                                                  true);
      bg2.start();

      BroadcastGroup bg3 = new BroadcastGroupImpl(node3,
                                                  RandomUtil.randomString(),
                                                  null,
                                                  -1,
                                                  groupAddress3,
                                                  groupPort3,
                                                  true);
      bg3.start();

      TransportConfiguration live1 = generateTC();
      TransportConfiguration backup1 = generateTC();

      TransportConfiguration live2 = generateTC();
      TransportConfiguration backup2 = generateTC();

      TransportConfiguration live3 = generateTC();
      TransportConfiguration backup3 = generateTC();

      Pair<TransportConfiguration, TransportConfiguration> connectorPair1 = new Pair<TransportConfiguration, TransportConfiguration>(live1,
                                                                                                                                     backup1);

      Pair<TransportConfiguration, TransportConfiguration> connectorPair2 = new Pair<TransportConfiguration, TransportConfiguration>(live2,
                                                                                                                                     backup2);

      Pair<TransportConfiguration, TransportConfiguration> connectorPair3 = new Pair<TransportConfiguration, TransportConfiguration>(live3,
                                                                                                                                     backup3);

      bg1.addConnectorPair(connectorPair1);
      bg2.addConnectorPair(connectorPair2);
      bg3.addConnectorPair(connectorPair3);

      DiscoveryGroup dg1 = new DiscoveryGroupImpl(RandomUtil.randomString(),
                                                  RandomUtil.randomString(),
                                                  null,
                                                  groupAddress1,
                                                  groupPort1,
                                                  timeout);
      dg1.start();

      DiscoveryGroup dg2 = new DiscoveryGroupImpl(RandomUtil.randomString(),
                                                  RandomUtil.randomString(),
                                                  null,
                                                  groupAddress2,
                                                  groupPort2,
                                                  timeout);
      dg2.start();

      DiscoveryGroup dg3 = new DiscoveryGroupImpl(RandomUtil.randomString(),
                                                  RandomUtil.randomString(),
                                                  null,
                                                  groupAddress3,
                                                  groupPort3,
                                                  timeout);
      dg3.start();

      bg1.broadcastConnectors();

      bg2.broadcastConnectors();

      bg3.broadcastConnectors();

      boolean ok = dg1.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      Map<String, DiscoveryEntry> entryMap = dg1.getDiscoveryEntryMap();
      Assert.assertNotNull(entryMap);
      Assert.assertEquals(1, entryMap.size());
      DiscoveryEntry entry = entryMap.get(node1);
      Assert.assertNotNull(entry);
      Assert.assertEquals(connectorPair1, entry.getConnectorPair());

      ok = dg2.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entryMap = dg2.getDiscoveryEntryMap();
      Assert.assertNotNull(entryMap);
      Assert.assertEquals(1, entryMap.size());
      entry = entryMap.get(node2);
      Assert.assertNotNull(entry);
      Assert.assertEquals(connectorPair2, entry.getConnectorPair());

      ok = dg3.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entryMap = dg3.getDiscoveryEntryMap();
      Assert.assertNotNull(entryMap);
      Assert.assertEquals(1, entryMap.size());
      entry = entryMap.get(node3);
      Assert.assertNotNull(entry);
      Assert.assertEquals(connectorPair3, entry.getConnectorPair());

      bg1.stop();
      bg2.stop();
      bg3.stop();

      dg1.stop();
      dg2.stop();
      dg3.stop();
   }

   public void testBroadcastNullBackup() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(DiscoveryTest.address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      String nodeID = RandomUtil.randomString();

      BroadcastGroup bg = new BroadcastGroupImpl(nodeID,
                                                 RandomUtil.randomString(),
                                                 null,
                                                 -1,
                                                 groupAddress,
                                                 groupPort,
                                                 true);

      bg.start();

      TransportConfiguration live1 = generateTC();

      Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration, TransportConfiguration>(live1,
                                                                                                                                    null);

      bg.addConnectorPair(connectorPair);

      DiscoveryGroup dg = new DiscoveryGroupImpl(RandomUtil.randomString(),
                                                 RandomUtil.randomString(),
                                                 null,
                                                 groupAddress,
                                                 groupPort,
                                                 timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      Assert.assertTrue(ok);

      Map<String, DiscoveryEntry> entryMap = dg.getDiscoveryEntryMap();
      Assert.assertNotNull(entryMap);
      Assert.assertEquals(1, entryMap.size());
      DiscoveryEntry entry = entryMap.get(nodeID);
      Assert.assertNotNull(entry);
      Assert.assertEquals(connectorPair, entry.getConnectorPair());

      bg.stop();

      dg.stop();

   }

   public void testDiscoveryListenersCalled() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(DiscoveryTest.address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      String nodeID = RandomUtil.randomString();

      BroadcastGroup bg = new BroadcastGroupImpl(nodeID,
                                                 RandomUtil.randomString(),
                                                 null,
                                                 -1,
                                                 groupAddress,
                                                 groupPort,
                                                 true);

      bg.start();

      TransportConfiguration live1 = generateTC();

      Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration, TransportConfiguration>(live1,
                                                                                                                                    null);

      bg.addConnectorPair(connectorPair);

      DiscoveryGroup dg = new DiscoveryGroupImpl(RandomUtil.randomString(),
                                                 RandomUtil.randomString(),
                                                 null,
                                                 groupAddress,
                                                 groupPort,
                                                 timeout);

      MyListener listener1 = new MyListener();
      MyListener listener2 = new MyListener();
      MyListener listener3 = new MyListener();

      dg.registerListener(listener1);
      dg.registerListener(listener2);
      dg.registerListener(listener3);

      dg.start();

      bg.broadcastConnectors();
      boolean ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);

      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      Assert.assertTrue(listener3.called);

      listener1.called = false;
      listener2.called = false;
      listener3.called = false;

      bg.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);

      // Won't be called since connectors haven't changed
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
      Assert.assertFalse(listener3.called);

      bg.stop();

      dg.stop();
   }

   public void testConnectorsUpdatedMultipleBroadcasters() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(DiscoveryTest.address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      String node1 = RandomUtil.randomString();
      String node2 = RandomUtil.randomString();
      String node3 = RandomUtil.randomString();

      BroadcastGroup bg1 = new BroadcastGroupImpl(node1,
                                                  RandomUtil.randomString(),
                                                  null,
                                                  -1,
                                                  groupAddress,
                                                  groupPort,
                                                  true);
      bg1.start();

      BroadcastGroup bg2 = new BroadcastGroupImpl(node2,
                                                  RandomUtil.randomString(),
                                                  null,
                                                  -1,
                                                  groupAddress,
                                                  groupPort,
                                                  true);
      bg2.start();

      BroadcastGroup bg3 = new BroadcastGroupImpl(node3,
                                                  RandomUtil.randomString(),
                                                  null,
                                                  -1,
                                                  groupAddress,
                                                  groupPort,
                                                  true);
      bg3.start();

      TransportConfiguration live1 = generateTC();
      TransportConfiguration backup1 = generateTC();
      Pair<TransportConfiguration, TransportConfiguration> connectorPair1 = new Pair<TransportConfiguration, TransportConfiguration>(live1,
                                                                                                                                     backup1);
      bg1.addConnectorPair(connectorPair1);

      TransportConfiguration live2 = generateTC();
      TransportConfiguration backup2 = generateTC();
      Pair<TransportConfiguration, TransportConfiguration> connectorPair2 = new Pair<TransportConfiguration, TransportConfiguration>(live2,
                                                                                                                                     backup2);
      bg2.addConnectorPair(connectorPair2);

      TransportConfiguration live3 = generateTC();
      TransportConfiguration backup3 = generateTC();
      Pair<TransportConfiguration, TransportConfiguration> connectorPair3 = new Pair<TransportConfiguration, TransportConfiguration>(live3,
                                                                                                                                     backup3);
      bg3.addConnectorPair(connectorPair3);

      DiscoveryGroup dg = new DiscoveryGroupImpl(RandomUtil.randomString(),
                                                 RandomUtil.randomString(),
                                                 null,
                                                 groupAddress,
                                                 groupPort,
                                                 timeout);

      MyListener listener1 = new MyListener();
      dg.registerListener(listener1);
      MyListener listener2 = new MyListener();
      dg.registerListener(listener2);

      dg.start();

      bg1.broadcastConnectors();
      boolean ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      Map<String, DiscoveryEntry> entryMap = dg.getDiscoveryEntryMap();
      Assert.assertNotNull(entryMap);
      Assert.assertEquals(1, entryMap.size());
      DiscoveryEntry entry = entryMap.get(node1);
      Assert.assertNotNull(entry);
      Assert.assertEquals(connectorPair1, entry.getConnectorPair());
      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entryMap = dg.getDiscoveryEntryMap();
      Assert.assertNotNull(entryMap);
      Assert.assertEquals(2, entryMap.size());
      DiscoveryEntry entry1 = entryMap.get(node1);
      Assert.assertNotNull(entry1);
      Assert.assertEquals(connectorPair1, entry1.getConnectorPair());
      DiscoveryEntry entry2 = entryMap.get(node2);
      Assert.assertNotNull(entry2);
      Assert.assertEquals(connectorPair2, entry2.getConnectorPair());
      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg3.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entryMap = dg.getDiscoveryEntryMap();
      Assert.assertNotNull(entryMap);
      Assert.assertEquals(3, entryMap.size());
      entry1 = entryMap.get(node1);
      Assert.assertNotNull(entry1);
      Assert.assertEquals(connectorPair1, entry1.getConnectorPair());
      entry2 = entryMap.get(node2);
      Assert.assertNotNull(entry2);
      Assert.assertEquals(connectorPair2, entry2.getConnectorPair());
      DiscoveryEntry entry3 = entryMap.get(node3);
      Assert.assertNotNull(entry3);
      Assert.assertEquals(connectorPair3, entry3.getConnectorPair());
      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg1.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entryMap = dg.getDiscoveryEntryMap();
      Assert.assertNotNull(entryMap);
      Assert.assertEquals(3, entryMap.size());
      entry1 = entryMap.get(node1);
      Assert.assertNotNull(entry1);
      Assert.assertEquals(connectorPair1, entry1.getConnectorPair());
      entry2 = entryMap.get(node2);
      Assert.assertNotNull(entry2);
      Assert.assertEquals(connectorPair2, entry2.getConnectorPair());
      entry3 = entryMap.get(node3);
      Assert.assertNotNull(entry3);
      Assert.assertEquals(connectorPair3, entry3.getConnectorPair());
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entryMap = dg.getDiscoveryEntryMap();
      Assert.assertNotNull(entryMap);
      Assert.assertEquals(3, entryMap.size());
      entry1 = entryMap.get(node1);
      Assert.assertNotNull(entry1);
      Assert.assertEquals(connectorPair1, entry1.getConnectorPair());
      entry2 = entryMap.get(node2);
      Assert.assertNotNull(entry2);
      Assert.assertEquals(connectorPair2, entry2.getConnectorPair());
      entry3 = entryMap.get(node3);
      Assert.assertNotNull(entry3);
      Assert.assertEquals(connectorPair3, entry3.getConnectorPair());
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg3.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entryMap = dg.getDiscoveryEntryMap();
      Assert.assertNotNull(entryMap);
      Assert.assertEquals(3, entryMap.size());
      entry1 = entryMap.get(node1);
      Assert.assertNotNull(entry1);
      Assert.assertEquals(connectorPair1, entry1.getConnectorPair());
      entry2 = entryMap.get(node2);
      Assert.assertNotNull(entry2);
      Assert.assertEquals(connectorPair2, entry2.getConnectorPair());
      entry3 = entryMap.get(node3);
      Assert.assertNotNull(entry3);
      Assert.assertEquals(connectorPair3, entry3.getConnectorPair());
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg2.removeConnectorPair(connectorPair2);
      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);

      // Connector2 should still be there since not timed out yet

      entryMap = dg.getDiscoveryEntryMap();
      Assert.assertNotNull(entryMap);
      Assert.assertEquals(3, entryMap.size());
      entry1 = entryMap.get(node1);
      Assert.assertNotNull(entry1);
      Assert.assertEquals(connectorPair1, entry1.getConnectorPair());
      entry2 = entryMap.get(node2);
      Assert.assertNotNull(entry2);
      Assert.assertEquals(connectorPair2, entry2.getConnectorPair());
      entry3 = entryMap.get(node3);
      Assert.assertNotNull(entry3);
      Assert.assertEquals(connectorPair3, entry3.getConnectorPair());

      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
      listener1.called = false;
      listener2.called = false;

      Thread.sleep(timeout);

      bg1.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg3.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);

      entryMap = dg.getDiscoveryEntryMap();
      Assert.assertNotNull(entryMap);
      Assert.assertEquals(2, entryMap.size());
      entry1 = entryMap.get(node1);
      Assert.assertNotNull(entry1);
      Assert.assertEquals(connectorPair1, entry1.getConnectorPair());
      entry3 = entryMap.get(node3);
      Assert.assertNotNull(entry3);
      Assert.assertEquals(connectorPair3, entry3.getConnectorPair());

      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg1.removeConnectorPair(connectorPair1);
      bg3.removeConnectorPair(connectorPair3);

      Thread.sleep(timeout);

      bg1.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg3.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);

      entryMap = dg.getDiscoveryEntryMap();
      Assert.assertNotNull(entryMap);
      Assert.assertEquals(0, entryMap.size());
      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg1.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg3.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);

      entryMap = dg.getDiscoveryEntryMap();
      Assert.assertNotNull(entryMap);
      Assert.assertEquals(0, entryMap.size());
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);

      bg1.stop();
      bg2.stop();
      bg3.stop();

      dg.stop();
   }

   public void testMultipleDiscoveryGroups() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(DiscoveryTest.address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      String nodeID = RandomUtil.randomString();

      BroadcastGroup bg = new BroadcastGroupImpl(nodeID,
                                                 RandomUtil.randomString(),
                                                 null,
                                                 -1,
                                                 groupAddress,
                                                 groupPort,
                                                 true);

      bg.start();

      TransportConfiguration live1 = generateTC();
      TransportConfiguration backup1 = generateTC();

      Pair<TransportConfiguration, TransportConfiguration> connectorPair1 = new Pair<TransportConfiguration, TransportConfiguration>(live1,
                                                                                                                                     backup1);

      bg.addConnectorPair(connectorPair1);

      DiscoveryGroup dg1 = new DiscoveryGroupImpl(RandomUtil.randomString(),
                                                  RandomUtil.randomString(),
                                                  null,
                                                  groupAddress,
                                                  groupPort,
                                                  timeout);

      DiscoveryGroup dg2 = new DiscoveryGroupImpl(RandomUtil.randomString(),
                                                  RandomUtil.randomString(),
                                                  null,
                                                  groupAddress,
                                                  groupPort,
                                                  timeout);

      DiscoveryGroup dg3 = new DiscoveryGroupImpl(RandomUtil.randomString(),
                                                  RandomUtil.randomString(),
                                                  null,
                                                  groupAddress,
                                                  groupPort,
                                                  timeout);

      dg1.start();
      dg2.start();
      dg3.start();

      bg.broadcastConnectors();

      boolean ok = dg1.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      Map<String, DiscoveryEntry> entryMap = dg1.getDiscoveryEntryMap();
      Assert.assertNotNull(entryMap);
      Assert.assertEquals(1, entryMap.size());
      DiscoveryEntry entry = entryMap.get(nodeID);
      Assert.assertNotNull(entry);
      Assert.assertEquals(connectorPair1, entry.getConnectorPair());

      ok = dg2.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entryMap = dg2.getDiscoveryEntryMap();
      Assert.assertNotNull(entryMap);
      Assert.assertEquals(1, entryMap.size());
      entry = entryMap.get(nodeID);
      Assert.assertNotNull(entry);
      Assert.assertEquals(connectorPair1, entry.getConnectorPair());

      ok = dg3.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entryMap = dg3.getDiscoveryEntryMap();
      Assert.assertNotNull(entryMap);
      Assert.assertEquals(1, entryMap.size());
      entry = entryMap.get(nodeID);
      Assert.assertNotNull(entry);
      Assert.assertEquals(connectorPair1, entry.getConnectorPair());

      bg.stop();

      dg1.stop();
      dg2.stop();
      dg3.stop();
   }

   public void testDiscoveryGroupNotifications() throws Exception
   {
      SimpleNotificationService notifService = new SimpleNotificationService();
      SimpleNotificationService.Listener notifListener = new SimpleNotificationService.Listener();
      notifService.addNotificationListener(notifListener);

      final InetAddress groupAddress = InetAddress.getByName(DiscoveryTest.address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      DiscoveryGroup dg = new DiscoveryGroupImpl(RandomUtil.randomString(),
                                                 RandomUtil.randomString(),
                                                 null,
                                                 groupAddress,
                                                 groupPort,
                                                 timeout);
      dg.setNotificationService(notifService);

      Assert.assertEquals(0, notifListener.getNotifications().size());

      dg.start();

      Assert.assertEquals(1, notifListener.getNotifications().size());
      Notification notif = notifListener.getNotifications().get(0);
      Assert.assertEquals(NotificationType.DISCOVERY_GROUP_STARTED, notif.getType());
      Assert.assertEquals(dg.getName(), notif.getProperties()
                                             .getSimpleStringProperty(new SimpleString("name"))
                                             .toString());

      dg.stop();

      Assert.assertEquals(2, notifListener.getNotifications().size());
      notif = notifListener.getNotifications().get(1);
      Assert.assertEquals(NotificationType.DISCOVERY_GROUP_STOPPED, notif.getType());
      Assert.assertEquals(dg.getName(), notif.getProperties()
                                             .getSimpleStringProperty(new SimpleString("name"))
                                             .toString());
   }

   public void testBroadcastGroupNotifications() throws Exception
   {
      SimpleNotificationService notifService = new SimpleNotificationService();
      SimpleNotificationService.Listener notifListener = new SimpleNotificationService.Listener();
      notifService.addNotificationListener(notifListener);

      final InetAddress groupAddress = InetAddress.getByName(DiscoveryTest.address1);
      final int groupPort = getUDPDiscoveryPort();

      BroadcastGroup bg = new BroadcastGroupImpl(RandomUtil.randomString(),
                                                 RandomUtil.randomString(),
                                                 null,
                                                 -1,
                                                 groupAddress,
                                                 groupPort,
                                                 true);
      bg.setNotificationService(notifService);

      Assert.assertEquals(0, notifListener.getNotifications().size());

      bg.start();

      Assert.assertEquals(1, notifListener.getNotifications().size());
      Notification notif = notifListener.getNotifications().get(0);
      Assert.assertEquals(NotificationType.BROADCAST_GROUP_STARTED, notif.getType());
      Assert.assertEquals(bg.getName(), notif.getProperties()
                                             .getSimpleStringProperty(new SimpleString("name"))
                                             .toString());

      bg.stop();

      Assert.assertEquals(2, notifListener.getNotifications().size());
      notif = notifListener.getNotifications().get(1);
      Assert.assertEquals(NotificationType.BROADCAST_GROUP_STOPPED, notif.getType());
      Assert.assertEquals(bg.getName(), notif.getProperties()
                                             .getSimpleStringProperty(new SimpleString("name"))
                                             .toString());
   }

   private TransportConfiguration generateTC()
   {
      String className = "org.foo.bar." + UUIDGenerator.getInstance().generateStringUUID();
      String name = UUIDGenerator.getInstance().generateStringUUID();
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(UUIDGenerator.getInstance().generateStringUUID(), 123);
      params.put(UUIDGenerator.getInstance().generateStringUUID(), UUIDGenerator.getInstance().generateStringUUID());
      params.put(UUIDGenerator.getInstance().generateStringUUID(), true);
      TransportConfiguration tc = new TransportConfiguration(className, params, name);
      return tc;
   }

   private static class MyListener implements DiscoveryListener
   {
      volatile boolean called;

      public void connectorsChanged()
      {
         called = true;
      }
   }

}
