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

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.hornetq.api.core.BroadcastEndpoint;
import org.hornetq.api.core.HornetQIllegalStateException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.cluster.DiscoveryEntry;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.cluster.DiscoveryListener;
import org.hornetq.core.config.UDPBroadcastGroupConfiguration;
import org.hornetq.core.config.JGroupsBroadcastGroupConfigurationWithFile;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.cluster.BroadcastGroup;
import org.hornetq.core.server.cluster.impl.BroadcastGroupImpl;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationService;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.integration.SimpleNotificationService;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.UUIDGenerator;

/**
 * This will test Discovery test on JGroups and UDP.
 *
 * In some configuration IPV6 may be a challenge.
 * To make sure this test works, you may add this property to your jvm settings:
 *
 * -Djgroups.bind_addr=::1
 *
 * Or ultimately you may also turn off IPV6:
 *
 * -Djava.net.preferIPv4Stack=true
 *
 *
 * Also: Make sure you add integration-tests/src/tests/resources to your project path on the tests/integration-tests
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
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private final String address1 = getUDPDiscoveryAddress();

   private final String address2 = getUDPDiscoveryAddress(1);

   private final String address3 = getUDPDiscoveryAddress(2);

   BroadcastGroup bg = null, bg1 = null, bg2 = null, bg3 = null;
   DiscoveryGroup dg = null, dg1 = null, dg2 = null, dg3 = null;


   protected void tearDown() throws Exception
   {
      try
      {
         if (bg != null) bg.stop();
         if (bg1 != null) bg1.stop();
         if (bg2 != null) bg2.stop();
         if (bg3 != null) bg3.stop();

         if (dg != null) dg.stop();
         if (dg1 != null) dg1.stop();
         if (dg2 != null) dg2.stop();
         if (dg3 != null) dg3.stop();
      }
      catch (Throwable ignored)
      {
         ignored.printStackTrace();
      }

      super.tearDown();
   }


   public void testSimpleBroadcast() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      final String nodeID = RandomUtil.randomString();

      bg = new BroadcastGroupImpl(new FakeNodeManager(nodeID),
         RandomUtil.randomString(),
         0,  null, new UDPBroadcastGroupConfiguration(address1, groupPort, null, -1).createBroadcastEndpointFactory());

      bg.start();

      TransportConfiguration live1 = generateTC();

      bg.addConnector(live1);

      dg = newDiscoveryGroup(RandomUtil.randomString(),
         RandomUtil.randomString(),
         null,
         groupAddress,
         groupPort,
         timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      Assert.assertTrue(ok);

      List<DiscoveryEntry> entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);
   }


   public void testSimpleBroadcastJGropus() throws Exception
   {
      final String nodeID = RandomUtil.randomString();

      bg = new BroadcastGroupImpl(new FakeNodeManager(nodeID), "broadcast", 100, null,
         new JGroupsBroadcastGroupConfigurationWithFile("test-jgroups-file_ping.xml", "tst").createBroadcastEndpointFactory());

      bg.start();

      TransportConfiguration live1 = generateTC();

      bg.addConnector(live1);

      dg = new DiscoveryGroup(nodeID + "1", "broadcast", 5000l,
            new JGroupsBroadcastGroupConfigurationWithFile("test-jgroups-file_ping.xml", "tst").createBroadcastEndpointFactory(),
            null);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(5000);

      Assert.assertTrue(ok);

      List<DiscoveryEntry> entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);
   }

   public void testStraightSendReceiveJGroups() throws Exception
   {
      BroadcastEndpoint broadcaster = null;
      BroadcastEndpoint client = null;
      try
      {
         JGroupsBroadcastGroupConfigurationWithFile jgroupsConfig = new JGroupsBroadcastGroupConfigurationWithFile("test-jgroups-file_ping.xml", "tst");
         broadcaster = jgroupsConfig.createBroadcastEndpointFactory().createBroadcastEndpoint();

         broadcaster.openBroadcaster();

         client = jgroupsConfig.createBroadcastEndpointFactory().createBroadcastEndpoint();

         client.openClient();

         Thread.sleep(1000);

         byte [] randomBytes = "PQP".getBytes();

         broadcaster.broadcast(randomBytes);

         byte [] btreceived = client.receiveBroadcast(5, TimeUnit.SECONDS);

         System.out.println("BTReceived = " + btreceived);

         assertNotNull(btreceived);

         assertEquals(randomBytes.length, btreceived.length);

         for (int i = 0; i < randomBytes.length; i++)
         {
            assertEquals(randomBytes[i], btreceived[i]);
         }
      }
      finally
      {
         try
         {
            if (broadcaster != null)
               broadcaster.close(true);

            if (client != null)
               client.close(false);
         }
         catch (Exception ignored)
         {
            ignored.printStackTrace();
         }
      }

   }

   public void testSimpleBroadcastSpecificNIC() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      final String nodeID = RandomUtil.randomString();

      // We need to choose a real NIC on the local machine - note this will silently pass if the machine
      // has no usable NIC!

      Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();

      InetAddress localAddress = null;

      outer: while (networkInterfaces.hasMoreElements())
      {
         NetworkInterface networkInterface = networkInterfaces.nextElement();
         if (networkInterface.isLoopback() || networkInterface.isVirtual() ||
            !networkInterface.isUp() ||
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

      bg = newBroadcast(nodeID,
         RandomUtil.randomString(),
         localAddress,
         6552,
         groupAddress,
         groupPort);

      bg.start();

      TransportConfiguration live1 = generateTC();

      bg.addConnector(live1);

      dg = newDiscoveryGroup(RandomUtil.randomString(),
         RandomUtil.randomString(),
         localAddress,
         groupAddress,
         groupPort,
         timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      Assert.assertTrue(ok);

      List<DiscoveryEntry> entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);

   }

   public void testSimpleBroadcastWithStopStartDiscoveryGroup() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      final String nodeID = RandomUtil.randomString();

      bg = newBroadcast(nodeID,
         RandomUtil.randomString(),
         null,
         -1,
         groupAddress,
         groupPort);

      bg.start();

      TransportConfiguration live1 = generateTC();

      bg.addConnector(live1);

      dg = newDiscoveryGroup(RandomUtil.randomString(),
         RandomUtil.randomString(),
         null,
         groupAddress,
         groupPort,
         timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      Assert.assertTrue(ok);

      List<DiscoveryEntry> entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);

      bg.stop();

      dg.stop();

      dg.start();

      bg.start();

      bg.broadcastConnectors();

      ok = dg.waitForBroadcast(1000);

      Assert.assertTrue(ok);

      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);
   }

   public void testIgnoreTrafficFromOwnNode() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      String nodeID = RandomUtil.randomString();

      bg = newBroadcast(nodeID,
         RandomUtil.randomString(),
         null,
         -1,
         groupAddress,
         groupPort);

      bg.start();

      TransportConfiguration live1 = generateTC();

      bg.addConnector(live1);

      dg = newDiscoveryGroup(nodeID,
         RandomUtil.randomString(),
         null,
         groupAddress,
         groupPort,
         timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      Assert.assertFalse(ok);

      List<DiscoveryEntry> entries = dg.getDiscoveryEntries();

      Assert.assertNotNull(entries);

      Assert.assertEquals(0, entries.size());

   }

   public void testSimpleBroadcastDifferentPort() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(getUDPDiscoveryAddress());
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      bg = newBroadcast(RandomUtil.randomString(),
         RandomUtil.randomString(),
         null,
         -1,
         groupAddress,
         groupPort);

      bg.start();

      TransportConfiguration live1 = generateTC();

      bg.addConnector(live1);

      final int port2 = getUDPDiscoveryPort(1);

      dg = newDiscoveryGroup(RandomUtil.randomString(),
         RandomUtil.randomString(),
         null,
         groupAddress,
         port2,
         timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      Assert.assertFalse(ok);
   }

   public void testSimpleBroadcastDifferentAddressAndPort() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      bg = newBroadcast(RandomUtil.randomString(),
         RandomUtil.randomString(),
         null,
         -1,
         groupAddress,
         groupPort);

      bg.start();

      TransportConfiguration live1 = generateTC();

      bg.addConnector(live1);

      final InetAddress groupAddress2 = InetAddress.getByName(address2);
      final int port2 = getUDPDiscoveryPort(1);

      dg = newDiscoveryGroup(RandomUtil.randomString(),
         RandomUtil.randomString(),
         null,
         groupAddress2,
         port2,
         timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      Assert.assertFalse(ok);

   }

   public void testMultipleGroups() throws Exception
   {
      final int groupPort1 = getUDPDiscoveryPort();

      final int groupPort2 = getUDPDiscoveryPort(1);

      final int groupPort3 = getUDPDiscoveryPort(2);

      final InetAddress groupAddress1 = InetAddress.getByName(address1);

      final InetAddress groupAddress2 = InetAddress.getByName(address2);

      final InetAddress groupAddress3 = InetAddress.getByName(address3);

      final int timeout = 5000;

      String node1 = UUIDGenerator.getInstance().generateStringUUID();

      String node2 = UUIDGenerator.getInstance().generateStringUUID();

      String node3 = UUIDGenerator.getInstance().generateStringUUID();

      bg1 = newBroadcast(node1,
         RandomUtil.randomString(),
         null,
         -1,
         groupAddress1,
         groupPort1);

      bg2 = newBroadcast(node2,
         RandomUtil.randomString(),
         null,
         -1,
         groupAddress2,
         groupPort2);

      bg3 = newBroadcast(node3,
         RandomUtil.randomString(),
         null,
         -1,
         groupAddress3,
         groupPort3);

      bg2.start();
      bg1.start();
      bg3.start();

      TransportConfiguration live1 = generateTC("live1");

      TransportConfiguration live2 = generateTC("live2");

      TransportConfiguration live3 = generateTC("live3");

      bg1.addConnector(live1);
      bg2.addConnector(live2);
      bg3.addConnector(live3);

      dg1 = newDiscoveryGroup("group-1::" + RandomUtil.randomString(),
         "group-1::" + RandomUtil.randomString(),
         null,
         groupAddress1,
         groupPort1,
         timeout);
      dg1.start();

      dg2 = newDiscoveryGroup("group-2::" + RandomUtil.randomString(),
         "group-2::" + RandomUtil.randomString(),
         null,
         groupAddress2,
         groupPort2,
         timeout);
      dg2.start();

      dg3 = newDiscoveryGroup("group-3::" + RandomUtil.randomString(),
         "group-3::" + RandomUtil.randomString(),
         null,
         groupAddress3,
         groupPort3,
         timeout);
      dg3.start();

      bg1.broadcastConnectors();

      bg2.broadcastConnectors();

      bg3.broadcastConnectors();

      boolean ok = dg1.waitForBroadcast(timeout);
      Assert.assertTrue(ok);
      List<DiscoveryEntry> entries = dg1.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);

      ok = dg2.waitForBroadcast(timeout);
      Assert.assertTrue(ok);
      entries = dg2.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live2), entries);

      ok = dg3.waitForBroadcast(timeout);
      Assert.assertTrue(ok);
      entries = dg3.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live3), entries);
   }

   // -- fix this test
   // public void testBroadcastNullBackup() throws Exception
   // {
   // final InetAddress groupAddress = InetAddress.getByName(address1);
   // final int groupPort = getUDPDiscoveryPort();
   // final int timeout = 500;
   //
   // String nodeID = RandomUtil.randomString();
   //
   // BroadcastGroup bg = new BroadcastGroupImpl(nodeID,
   // RandomUtil.randomString(),
   // null,
   // -1,
   // groupAddress,
   // groupPort,
   // true);
   //
   // bg.start();
   //
   // TransportConfiguration live1 = generateTC();
   //
   // Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration,
   // TransportConfiguration>(live1,
   // null);
   //
   // bg.addConnectorPair(connectorPair);
   //
   // DiscoveryGroup dg = new DiscoveryGroup(RandomUtil.randomString(),
   // RandomUtil.randomString(),
   // null,
   // groupAddress,
   // groupPort,
   // timeout,
   // Executors.newFixedThreadPool(1));
   //
   // dg.start();
   //
   // bg.broadcastConnectors();
   //
   // boolean ok = dg.waitForBroadcast(1000);
   //
   // Assert.assertTrue(ok);
   // }

   public void testDiscoveryListenersCalled() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      String nodeID = RandomUtil.randomString();

      bg = newBroadcast(nodeID,
         RandomUtil.randomString(),
         null,
         -1,
         groupAddress,
         groupPort);

      bg.start();

      TransportConfiguration live1 = generateTC();

      bg.addConnector(live1);

      dg = newDiscoveryGroup(RandomUtil.randomString(),
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
   }

   public void testConnectorsUpdatedMultipleBroadcasters() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      String node1 = RandomUtil.randomString();
      String node2 = RandomUtil.randomString();
      String node3 = RandomUtil.randomString();

      bg1 = newBroadcast(node1,
         RandomUtil.randomString(),
         null,
         -1,
         groupAddress,
         groupPort);
      bg1.start();

      bg2 = newBroadcast(node2,
         RandomUtil.randomString(),
         null,
         -1,
         groupAddress,
         groupPort);
      bg2.start();

      bg3 = newBroadcast(node3,
         RandomUtil.randomString(),
         null,
         -1,
         groupAddress,
         groupPort);
      bg3.start();

      TransportConfiguration live1 = generateTC();
      bg1.addConnector(live1);

      TransportConfiguration live2 = generateTC();
      bg2.addConnector(live2);

      TransportConfiguration live3 = generateTC();
      bg3.addConnector(live3);

      dg = newDiscoveryGroup(RandomUtil.randomString(),
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
      List<DiscoveryEntry> entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);
      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live2), entries);
      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg3.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live2, live3), entries);
      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg1.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live2, live3), entries);
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live2, live3), entries);
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg3.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live2, live3), entries);
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg2.removeConnector(live2);
      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      Assert.assertTrue(ok);

      // Connector2 should still be there since not timed out yet

      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live2, live3), entries);
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
      listener1.called = false;
      listener2.called = false;

      Thread.sleep(timeout * 2);

      bg1.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg3.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);

      entries = dg.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1, live3), entries);
      Assert.assertTrue(listener1.called);
      Assert.assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg1.removeConnector(live1);
      bg3.removeConnector(live3);

      Thread.sleep(timeout * 2);

      bg1.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg3.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);

      entries = dg.getDiscoveryEntries();
      Assert.assertNotNull(entries);
      Assert.assertEquals(0, entries.size());
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

      entries = dg.getDiscoveryEntries();
      Assert.assertNotNull(entries);
      Assert.assertEquals(0, entries.size());
      Assert.assertFalse(listener1.called);
      Assert.assertFalse(listener2.called);
   }

   public void testMultipleDiscoveryGroups() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      String nodeID = RandomUtil.randomString();

      bg = newBroadcast(nodeID,
         RandomUtil.randomString(),
         null,
         -1,
         groupAddress,
         groupPort);

      bg.start();

      TransportConfiguration live1 = generateTC();

      bg.addConnector(live1);

      dg1 = newDiscoveryGroup(RandomUtil.randomString(),
         RandomUtil.randomString(),
         null,
         groupAddress,
         groupPort,
         timeout);

      dg2 = newDiscoveryGroup(RandomUtil.randomString(),
         RandomUtil.randomString(),
         null,
         groupAddress,
         groupPort,
         timeout);

      dg3 = newDiscoveryGroup(RandomUtil.randomString(),
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
      List<DiscoveryEntry> entries = dg1.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);

      ok = dg2.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entries = dg2.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);

      ok = dg3.waitForBroadcast(1000);
      Assert.assertTrue(ok);
      entries = dg3.getDiscoveryEntries();
      assertEqualsDiscoveryEntries(Arrays.asList(live1), entries);

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

      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();
      final int timeout = 500;

      dg = newDiscoveryGroup(RandomUtil.randomString(),
         RandomUtil.randomString(),
         null,
         groupAddress,
         groupPort,
         timeout, notifService);

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

      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = getUDPDiscoveryPort();

      bg = newBroadcast(RandomUtil.randomString(),
         RandomUtil.randomString(),
         null,
         -1,
         groupAddress,
         groupPort);

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

   private TransportConfiguration generateTC(String debug)
   {
      String className = "org.foo.bar." + debug + "|" + UUIDGenerator.getInstance().generateStringUUID() + "";
      String name = UUIDGenerator.getInstance().generateStringUUID();
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(UUIDGenerator.getInstance().generateStringUUID(), 123);
      params.put(UUIDGenerator.getInstance().generateStringUUID(), UUIDGenerator.getInstance().generateStringUUID());
      params.put(UUIDGenerator.getInstance().generateStringUUID(), true);
      TransportConfiguration tc = new TransportConfiguration(className, params, name);
      return tc;
   }

   private TransportConfiguration generateTC()
   {
      return generateTC("");
   }

   private static class MyListener implements DiscoveryListener
   {
      volatile boolean called;

      public void connectorsChanged(List<DiscoveryEntry> newConnectors)
      {
         called = true;
      }
   }

   private static void assertEqualsDiscoveryEntries(List<TransportConfiguration> expected, List<DiscoveryEntry> actual)
   {
      assertNotNull(actual);

      List<TransportConfiguration> sortedExpected = new ArrayList<TransportConfiguration>(expected);
      Collections.sort(sortedExpected, new Comparator<TransportConfiguration>()
      {

         public int compare(TransportConfiguration o1, TransportConfiguration o2)
         {
            return o2.toString().compareTo(o1.toString());
         }
      });
      List<DiscoveryEntry> sortedActual = new ArrayList<DiscoveryEntry>(actual);
      Collections.sort(sortedActual, new Comparator<DiscoveryEntry>()
      {
         public int compare(DiscoveryEntry o1, DiscoveryEntry o2)
         {
            return o2.getConnector().toString().compareTo(o1.getConnector().toString());
         }
      });
      if (sortedExpected.size() != sortedActual.size())
      {
         dump(sortedExpected, sortedActual);
      }
      assertEquals(sortedExpected.size(), sortedActual.size());
      for (int i = 0; i < sortedExpected.size(); i++)
      {
         if (!sortedExpected.get(i).equals(sortedActual.get(i).getConnector()))
         {
            dump(sortedExpected, sortedActual);
         }
         assertEquals(sortedExpected.get(i), sortedActual.get(i).getConnector());
      }
   }

   private static void dump(List<TransportConfiguration> sortedExpected, List<DiscoveryEntry> sortedActual)
   {
      System.out.println("wrong broadcasts received");
      System.out.println("expected");
      System.out.println("----------------------------");
      for (TransportConfiguration transportConfiguration : sortedExpected)
      {
         System.out.println("transportConfiguration = " + transportConfiguration);
      }
      System.out.println("----------------------------");
      System.out.println("actual");
      System.out.println("----------------------------");
      for (DiscoveryEntry discoveryEntry : sortedActual)
      {
         System.out.println("transportConfiguration = " + discoveryEntry.getConnector());
      }
      System.out.println("----------------------------");
   }

   /**
    * This method is here just to facilitate creating the Broadcaster for this test
    */
   private BroadcastGroupImpl newBroadcast(final String nodeID,
                                           final String name,
                                           final InetAddress localAddress,
                                           int localPort,
                                           final InetAddress groupAddress,
                                           final int groupPort) throws Exception
   {
      return new BroadcastGroupImpl(new FakeNodeManager(nodeID), name, 0, null,
         new UDPBroadcastGroupConfiguration(groupAddress.getHostAddress(), groupPort,
               localAddress != null?localAddress.getHostAddress():null, localPort).createBroadcastEndpointFactory());
   }

   private DiscoveryGroup newDiscoveryGroup(final String nodeID, final String name, final InetAddress localBindAddress,
                                            final InetAddress groupAddress, final int groupPort, final long timeout) throws Exception
   {
      return newDiscoveryGroup(nodeID, name, localBindAddress, groupAddress, groupPort, timeout, null);
   }

   private DiscoveryGroup newDiscoveryGroup(final String nodeID, final String name, final InetAddress localBindAddress,
                                            final InetAddress groupAddress, final int groupPort, final long timeout, NotificationService notif) throws Exception
   {
      return new DiscoveryGroup(nodeID, name, timeout, 
            new UDPBroadcastGroupConfiguration(groupAddress.getHostAddress(), groupPort,
                  localBindAddress != null?localBindAddress.getHostAddress(): null, -1).createBroadcastEndpointFactory(), notif);
   }


   private class FakeNodeManager extends NodeManager
   {

      public FakeNodeManager(String nodeID)
      {
         this.setNodeID(nodeID);
      }


      @Override
      public void awaitLiveNode() throws Exception
      {
      }

      @Override
      public void startBackup() throws Exception
      {
      }

      @Override
      public void startLiveNode() throws Exception
      {
      }

      @Override
      public void pauseLiveServer() throws Exception
      {
      }

      @Override
      public void crashLiveServer() throws Exception
      {
      }

      @Override
      public void stopBackup() throws Exception
      {
      }

      @Override
      public void releaseBackup() throws Exception
      {
      }

      @Override
      public void start() throws Exception
      {
         super.start();
      }

      @Override
      public void stop() throws Exception
      {
         super.stop();
      }

      @Override
      public boolean isStarted()
      {
         return super.isStarted();
      }

      @Override
      public SimpleString getNodeId()
      {
         return super.getNodeId();
      }

      @Override
      public SimpleString readNodeId() throws HornetQIllegalStateException, IOException
      {
         return null;
      }

      @Override
      public boolean isAwaitingFailback() throws Exception
      {
         return false;
      }

      @Override
      public boolean isBackupLive() throws Exception
      {
         return false;
      }

      @Override
      public void interrupt()
      {
      }
   }

}
