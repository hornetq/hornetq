/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.integration.discovery;

import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.jboss.messaging.core.cluster.DiscoveryEntry;
import org.jboss.messaging.core.cluster.DiscoveryGroup;
import org.jboss.messaging.core.cluster.DiscoveryListener;
import org.jboss.messaging.core.cluster.impl.DiscoveryGroupImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.cluster.BroadcastGroup;
import org.jboss.messaging.core.server.cluster.impl.BroadcastGroupImpl;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.Pair;
import org.jboss.messaging.utils.UUIDGenerator;

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
   
   private static final String address1 = "230.1.2.3";
   
   private static final String address2 = "230.1.2.4";
   
   private static final String address3 = "230.1.2.5";
   
   private static final String address4 = "230.1.2.6";

   public void testSimpleBroadcast() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = 6745;
      final int timeout = 500;
      
      final String nodeID = randomString();

      BroadcastGroup bg = new BroadcastGroupImpl(nodeID, randomString(), null, -1, groupAddress, groupPort, true);

      bg.start();

      TransportConfiguration live1 = generateTC();

      TransportConfiguration backup1 = generateTC();

      Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration, TransportConfiguration>(live1,
                                                                                                                                    backup1);

      bg.addConnectorPair(connectorPair);

      DiscoveryGroup dg = new DiscoveryGroupImpl(randomString(), randomString(), groupAddress, groupPort, timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      assertTrue(ok);

      Map<String, DiscoveryEntry> entryMap = dg.getDiscoveryEntryMap();

      assertNotNull(entryMap);

      assertEquals(1, entryMap.size());

      DiscoveryEntry entry = entryMap.get(nodeID);
      
      assertNotNull(entry);

      assertEquals(connectorPair, entry.getConnectorPair());

      bg.stop();

      dg.stop();

   }
   
   public void testSimpleBroadcastWithStopStartDiscoveryGroup() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = 6745;
      final int timeout = 500;
      
      final String nodeID = randomString();

      BroadcastGroup bg = new BroadcastGroupImpl(nodeID, randomString(), null, -1, groupAddress, groupPort, true);

      bg.start();

      TransportConfiguration live1 = generateTC();

      TransportConfiguration backup1 = generateTC();

      Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration, TransportConfiguration>(live1,
                                                                                                                                    backup1);

      bg.addConnectorPair(connectorPair);

      DiscoveryGroup dg = new DiscoveryGroupImpl(randomString(), randomString(), groupAddress, groupPort, timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      assertTrue(ok);

      Map<String, DiscoveryEntry> entryMap = dg.getDiscoveryEntryMap();

      assertNotNull(entryMap);

      assertEquals(1, entryMap.size());

      DiscoveryEntry entry = entryMap.get(nodeID);
      
      assertNotNull(entry);

      assertEquals(connectorPair, entry.getConnectorPair());

      bg.stop();

      dg.stop();
      
      dg.start();
                
      bg.start();
      
      bg.broadcastConnectors();
      
      ok = dg.waitForBroadcast(1000);

      assertTrue(ok);

      entryMap = dg.getDiscoveryEntryMap();

      assertNotNull(entryMap);

      assertEquals(1, entryMap.size());

      entry = entryMap.get(nodeID);
      
      assertNotNull(entry);

      assertEquals(connectorPair, entry.getConnectorPair());

   }
   
   public void testIgnoreTrafficFromOwnNode() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = 6745;
      final int timeout = 500;
      
      String nodeID = randomString();

      BroadcastGroup bg = new BroadcastGroupImpl(nodeID, randomString(), null, -1, groupAddress, groupPort, true);

      bg.start();

      TransportConfiguration live1 = generateTC();

      TransportConfiguration backup1 = generateTC();

      Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration, TransportConfiguration>(live1,
                                                                                                                                    backup1);

      bg.addConnectorPair(connectorPair);

      DiscoveryGroup dg = new DiscoveryGroupImpl(nodeID, randomString(), groupAddress, groupPort, timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      assertFalse(ok);

      Map<String, DiscoveryEntry> entryMap = dg.getDiscoveryEntryMap();

      assertNotNull(entryMap);

      assertEquals(0, entryMap.size());

      bg.stop();

      dg.stop();

   }

// There is a bug in some OSes where different addresses but *Same port* will receive the traffic - hence this test won't pass
//   See http://www.jboss.org/community/docs/DOC-11710 (jboss wiki promiscuous traffic)
   
   
//   public void testSimpleBroadcastDifferentAddress() throws Exception
//   {
//      final InetAddress groupAddress = InetAddress.getByName(address1);
//      final int groupPort = 6745;
//      final int timeout = 500;
//
//      BroadcastGroup bg = new BroadcastGroupImpl(randomString(), randomString(), null, -1, groupAddress, groupPort);
//
//      bg.start();
//
//      TransportConfiguration live1 = generateTC();
//
//      TransportConfiguration backup1 = generateTC();
//
//      Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration, TransportConfiguration>(live1,
//                                                                                                                                    backup1);
//
//      bg.addConnectorPair(connectorPair);
//
//      final InetAddress groupAddress2 = InetAddress.getByName(address2);
//
//      DiscoveryGroup dg = new DiscoveryGroupImpl(randomString(), randomString(), groupAddress2, groupPort, timeout);
//
//      dg.start();
//
//      bg.broadcastConnectors();
//
//      boolean ok = dg.waitForBroadcast(1000);
//
//      assertFalse(ok);
//
//      bg.stop();
//
//      dg.stop();
//
//   }

   public void testSimpleBroadcastDifferentPort() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName("230.1.2.3");
      final int groupPort = 6745;
      final int timeout = 500;

      BroadcastGroup bg = new BroadcastGroupImpl(randomString(), randomString(), null, -1, groupAddress, groupPort, true);

      bg.start();

      TransportConfiguration live1 = generateTC();

      TransportConfiguration backup1 = generateTC();

      Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration, TransportConfiguration>(live1,
                                                                                                                                    backup1);

      bg.addConnectorPair(connectorPair);

      final int port2 = 6746;

      DiscoveryGroup dg = new DiscoveryGroupImpl(randomString(), randomString(), groupAddress, port2, timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      assertFalse(ok);

      bg.stop();

      dg.stop();
   }

   public void testSimpleBroadcastDifferentAddressAndPort() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = 6745;
      final int timeout = 500;

      BroadcastGroup bg = new BroadcastGroupImpl(randomString(), randomString(), null, -1, groupAddress, groupPort, true);

      bg.start();

      TransportConfiguration live1 = generateTC();

      TransportConfiguration backup1 = generateTC();

      Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration, TransportConfiguration>(live1,
                                                                                                                                    backup1);

      bg.addConnectorPair(connectorPair);

      final InetAddress groupAddress2 = InetAddress.getByName(address2);
      final int port2 = 6746;

      DiscoveryGroup dg = new DiscoveryGroupImpl(randomString(), randomString(), groupAddress2, port2, timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      assertFalse(ok);

      bg.stop();

      dg.stop();
   }

   public void testMultipleGroups() throws Exception
   {
      final InetAddress groupAddress1 = InetAddress.getByName(address1);
      final int groupPort1 = 6745;

      final InetAddress groupAddress2 = InetAddress.getByName(address2);
      final int groupPort2 = 6746;

      final InetAddress groupAddress3 = InetAddress.getByName(address3);
      final int groupPort3 = 6747;

      final int timeout = 500;
      
      String node1 = randomString();
      
      String node2 = randomString();
      
      String node3 = randomString();

      BroadcastGroup bg1 = new BroadcastGroupImpl(node1, randomString(), null, -1, groupAddress1, groupPort1, true);
      bg1.start();

      BroadcastGroup bg2 = new BroadcastGroupImpl(node2, randomString(), null, -1, groupAddress2, groupPort2, true);
      bg2.start();

      BroadcastGroup bg3 = new BroadcastGroupImpl(node3, randomString(), null, -1, groupAddress3, groupPort3, true);
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

      DiscoveryGroup dg1 = new DiscoveryGroupImpl(randomString(), randomString(), groupAddress1, groupPort1, timeout);
      dg1.start();

      DiscoveryGroup dg2 = new DiscoveryGroupImpl(randomString(), randomString(), groupAddress2, groupPort2, timeout);
      dg2.start();

      DiscoveryGroup dg3 = new DiscoveryGroupImpl(randomString(), randomString(), groupAddress3, groupPort3, timeout);
      dg3.start();

      bg1.broadcastConnectors();

      bg2.broadcastConnectors();

      bg3.broadcastConnectors();

      boolean ok = dg1.waitForBroadcast(1000);
      assertTrue(ok);
      Map<String, DiscoveryEntry> entryMap = dg1.getDiscoveryEntryMap();
      assertNotNull(entryMap);
      assertEquals(1, entryMap.size());
      DiscoveryEntry entry = entryMap.get(node1);      
      assertNotNull(entry);
      assertEquals(connectorPair1, entry.getConnectorPair());

      ok = dg2.waitForBroadcast(1000);
      assertTrue(ok);
      entryMap = dg2.getDiscoveryEntryMap();
      assertNotNull(entryMap);
      assertEquals(1, entryMap.size());
      entry = entryMap.get(node2);      
      assertNotNull(entry);
      assertEquals(connectorPair2, entry.getConnectorPair());

      ok = dg3.waitForBroadcast(1000);
      assertTrue(ok);
      entryMap = dg3.getDiscoveryEntryMap();
      assertNotNull(entryMap);
      assertEquals(1, entryMap.size());
      entry = entryMap.get(node3);      
      assertNotNull(entry);
      assertEquals(connectorPair3, entry.getConnectorPair());

      bg1.stop();
      bg2.stop();
      bg3.stop();

      dg1.stop();
      dg2.stop();
      dg3.stop();
   }

   public void testBroadcastNullBackup() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = 6745;
      final int timeout = 500;
      
      String nodeID = randomString();

      BroadcastGroup bg = new BroadcastGroupImpl(nodeID, randomString(), null, -1, groupAddress, groupPort, true);

      bg.start();

      TransportConfiguration live1 = generateTC();

      Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration, TransportConfiguration>(live1,
                                                                                                                                    null);

      bg.addConnectorPair(connectorPair);

      DiscoveryGroup dg = new DiscoveryGroupImpl(randomString(), randomString(), groupAddress, groupPort, timeout);

      dg.start();

      bg.broadcastConnectors();

      boolean ok = dg.waitForBroadcast(1000);

      assertTrue(ok);

      Map<String, DiscoveryEntry> entryMap = dg.getDiscoveryEntryMap();
      assertNotNull(entryMap);
      assertEquals(1, entryMap.size());
      DiscoveryEntry entry = entryMap.get(nodeID);      
      assertNotNull(entry);
      assertEquals(connectorPair, entry.getConnectorPair());

      bg.stop();

      dg.stop();

   }

   public void testDiscoveryListenersCalled() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = 6745;
      final int timeout = 500;
      
      String nodeID = randomString();

      BroadcastGroup bg = new BroadcastGroupImpl(nodeID, randomString(), null, -1, groupAddress, groupPort, true);

      bg.start();

      TransportConfiguration live1 = generateTC();

      Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration, TransportConfiguration>(live1,
                                                                                                                                    null);

      bg.addConnectorPair(connectorPair);

      DiscoveryGroup dg = new DiscoveryGroupImpl(randomString(), randomString(), groupAddress, groupPort, timeout);

      MyListener listener1 = new MyListener();
      MyListener listener2 = new MyListener();
      MyListener listener3 = new MyListener();

      dg.registerListener(listener1);
      dg.registerListener(listener2);
      dg.registerListener(listener3);

      dg.start();

      bg.broadcastConnectors();
      boolean ok = dg.waitForBroadcast(1000);
      assertTrue(ok);

      assertTrue(listener1.called);
      assertTrue(listener2.called);
      assertTrue(listener3.called);

      listener1.called = false;
      listener2.called = false;
      listener3.called = false;

      bg.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      assertTrue(ok);

      // Won't be called since connectors haven't changed
      assertFalse(listener1.called);
      assertFalse(listener2.called);
      assertFalse(listener3.called);

      bg.stop();

      dg.stop();
   }

   public void testConnectorsUpdatedMultipleBroadcasters() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = 6745;
      final int timeout = 500;
      
      String node1 = randomString();
      String node2 = randomString();
      String node3 = randomString();

      BroadcastGroup bg1 = new BroadcastGroupImpl(node1, randomString(), null, -1, groupAddress, groupPort, true);
      bg1.start();

      BroadcastGroup bg2 = new BroadcastGroupImpl(node2, randomString(), null, -1, groupAddress, groupPort, true);
      bg2.start();

      BroadcastGroup bg3 = new BroadcastGroupImpl(node3, randomString(), null, -1, groupAddress, groupPort, true);
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

      DiscoveryGroup dg = new DiscoveryGroupImpl(randomString(), randomString(), groupAddress, groupPort, timeout);

      MyListener listener1 = new MyListener();
      dg.registerListener(listener1);
      MyListener listener2 = new MyListener();
      dg.registerListener(listener2);

      dg.start();

      bg1.broadcastConnectors();
      boolean ok = dg.waitForBroadcast(1000);
      assertTrue(ok);
      Map<String, DiscoveryEntry> entryMap = dg.getDiscoveryEntryMap();
      assertNotNull(entryMap);
      assertEquals(1, entryMap.size());
      DiscoveryEntry entry = entryMap.get(node1);      
      assertNotNull(entry);
      assertEquals(connectorPair1, entry.getConnectorPair());
      assertTrue(listener1.called);
      assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      assertTrue(ok);
      entryMap = dg.getDiscoveryEntryMap();
      assertNotNull(entryMap);
      assertEquals(2, entryMap.size());      
      DiscoveryEntry entry1 = entryMap.get(node1);      
      assertNotNull(entry1);
      assertEquals(connectorPair1, entry1.getConnectorPair());
      DiscoveryEntry entry2 = entryMap.get(node2);      
      assertNotNull(entry2);
      assertEquals(connectorPair2, entry2.getConnectorPair());
      assertTrue(listener1.called);
      assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg3.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      assertTrue(ok);
      entryMap = dg.getDiscoveryEntryMap();
      assertNotNull(entryMap);
      assertEquals(3, entryMap.size());      
      entry1 = entryMap.get(node1);      
      assertNotNull(entry1);
      assertEquals(connectorPair1, entry1.getConnectorPair());
      entry2 = entryMap.get(node2);      
      assertNotNull(entry2);
      assertEquals(connectorPair2, entry2.getConnectorPair());
      DiscoveryEntry entry3 = entryMap.get(node3);      
      assertNotNull(entry3);
      assertEquals(connectorPair3, entry3.getConnectorPair());
      assertTrue(listener1.called);
      assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg1.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      assertTrue(ok);
      entryMap = dg.getDiscoveryEntryMap();
      assertNotNull(entryMap);
      assertEquals(3, entryMap.size());      
      entry1 = entryMap.get(node1);      
      assertNotNull(entry1);
      assertEquals(connectorPair1, entry1.getConnectorPair());
      entry2 = entryMap.get(node2);      
      assertNotNull(entry2);
      assertEquals(connectorPair2, entry2.getConnectorPair());
      entry3 = entryMap.get(node3);      
      assertNotNull(entry3);
      assertEquals(connectorPair3, entry3.getConnectorPair());
      assertFalse(listener1.called);
      assertFalse(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      assertTrue(ok);
      entryMap = dg.getDiscoveryEntryMap();
      assertNotNull(entryMap);
      assertEquals(3, entryMap.size());      
      entry1 = entryMap.get(node1);      
      assertNotNull(entry1);
      assertEquals(connectorPair1, entry1.getConnectorPair());
      entry2 = entryMap.get(node2);      
      assertNotNull(entry2);
      assertEquals(connectorPair2, entry2.getConnectorPair());
      entry3 = entryMap.get(node3);      
      assertNotNull(entry3);
      assertEquals(connectorPair3, entry3.getConnectorPair());
      assertFalse(listener1.called);
      assertFalse(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg3.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      assertTrue(ok);
      entryMap = dg.getDiscoveryEntryMap();
      assertNotNull(entryMap);
      assertEquals(3, entryMap.size());      
      entry1 = entryMap.get(node1);      
      assertNotNull(entry1);
      assertEquals(connectorPair1, entry1.getConnectorPair());
      entry2 = entryMap.get(node2);      
      assertNotNull(entry2);
      assertEquals(connectorPair2, entry2.getConnectorPair());
      entry3 = entryMap.get(node3);      
      assertNotNull(entry3);
      assertEquals(connectorPair3, entry3.getConnectorPair());
      assertFalse(listener1.called);
      assertFalse(listener2.called);
      listener1.called = false;
      listener2.called = false;
    
      bg2.removeConnectorPair(connectorPair2);
      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      assertTrue(ok);

      // Connector2 should still be there since not timed out yet

      entryMap = dg.getDiscoveryEntryMap();
      assertNotNull(entryMap);
      assertEquals(3, entryMap.size());      
      entry1 = entryMap.get(node1);      
      assertNotNull(entry1);
      assertEquals(connectorPair1, entry1.getConnectorPair());
      entry2 = entryMap.get(node2);      
      assertNotNull(entry2);
      assertEquals(connectorPair2, entry2.getConnectorPair());
      entry3 = entryMap.get(node3);      
      assertNotNull(entry3);
      assertEquals(connectorPair3, entry3.getConnectorPair());
      
      assertFalse(listener1.called);
      assertFalse(listener2.called);
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
      assertNotNull(entryMap);
      assertEquals(2, entryMap.size());      
      entry1 = entryMap.get(node1);      
      assertNotNull(entry1);
      assertEquals(connectorPair1, entry1.getConnectorPair());     
      entry3 = entryMap.get(node3);      
      assertNotNull(entry3);
      assertEquals(connectorPair3, entry3.getConnectorPair());
      
      assertTrue(listener1.called);
      assertTrue(listener2.called);
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
      assertNotNull(entryMap);
      assertEquals(0, entryMap.size());           
      assertTrue(listener1.called);
      assertTrue(listener2.called);
      listener1.called = false;
      listener2.called = false;

      bg1.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg2.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);
      bg3.broadcastConnectors();
      ok = dg.waitForBroadcast(1000);

      entryMap = dg.getDiscoveryEntryMap();
      assertNotNull(entryMap);
      assertEquals(0, entryMap.size());   
      assertFalse(listener1.called);
      assertFalse(listener2.called);

      bg1.stop();
      bg2.stop();
      bg3.stop();

      dg.stop();
   }

   public void testMultipleDiscoveryGroups() throws Exception
   {
      final InetAddress groupAddress = InetAddress.getByName(address1);
      final int groupPort = 6745;
      final int timeout = 500;
      
      String nodeID = randomString();

      BroadcastGroup bg = new BroadcastGroupImpl(nodeID, randomString(), null, -1, groupAddress, groupPort, true);

      bg.start();

      TransportConfiguration live1 = generateTC();
      TransportConfiguration backup1 = generateTC();

      Pair<TransportConfiguration, TransportConfiguration> connectorPair1 = new Pair<TransportConfiguration, TransportConfiguration>(live1,
                                                                                                                                     backup1);

      bg.addConnectorPair(connectorPair1);
            

      DiscoveryGroup dg1 = new DiscoveryGroupImpl(randomString(), randomString(), groupAddress, groupPort, timeout);

      DiscoveryGroup dg2 = new DiscoveryGroupImpl(randomString(), randomString(), groupAddress, groupPort, timeout);

      DiscoveryGroup dg3 = new DiscoveryGroupImpl(randomString(), randomString(), groupAddress, groupPort, timeout);

      dg1.start();
      dg2.start();
      dg3.start();

      bg.broadcastConnectors();

      boolean ok = dg1.waitForBroadcast(1000);
      assertTrue(ok);
      Map<String, DiscoveryEntry> entryMap = dg1.getDiscoveryEntryMap();
      assertNotNull(entryMap);
      assertEquals(1, entryMap.size());
      DiscoveryEntry entry = entryMap.get(nodeID);      
      assertNotNull(entry);
      assertEquals(connectorPair1, entry.getConnectorPair());

      ok = dg2.waitForBroadcast(1000);
      assertTrue(ok);
      entryMap = dg2.getDiscoveryEntryMap();
      assertNotNull(entryMap);
      assertEquals(1, entryMap.size());
      entry = entryMap.get(nodeID);      
      assertNotNull(entry);
      assertEquals(connectorPair1, entry.getConnectorPair());
      
      
      ok = dg3.waitForBroadcast(1000);
      assertTrue(ok);
      entryMap = dg3.getDiscoveryEntryMap();
      assertNotNull(entryMap);
      assertEquals(1, entryMap.size());
      entry = entryMap.get(nodeID);      
      assertNotNull(entry);
      assertEquals(connectorPair1, entry.getConnectorPair());
      
      bg.stop();

      dg1.stop();
      dg2.stop();
      dg3.stop();
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
