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


package org.jboss.messaging.tests.integration.cluster.distribution;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.messaging.core.cluster.DiscoveryGroup;
import org.jboss.messaging.core.cluster.DiscoveryListener;
import org.jboss.messaging.core.cluster.impl.DiscoveryGroupImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.server.cluster.BroadcastGroup;
import org.jboss.messaging.core.server.cluster.impl.BroadcastGroupImpl;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.Pair;
import org.jboss.messaging.util.UUIDGenerator;

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
   public void testSimpleBroadcast() throws Exception
   {      
      final InetAddress groupAddress = InetAddress.getByName("230.1.2.3");
      final int groupPort = 6745;
      final int timeout = 500;
      
      BroadcastGroup bg = new BroadcastGroupImpl(null, -1, groupAddress, groupPort);
      
      bg.start();
      
      TransportConfiguration live1 = generateTC();
      
      TransportConfiguration backup1 = generateTC();
      
      Pair<TransportConfiguration, TransportConfiguration> connectorPair = 
         new Pair<TransportConfiguration, TransportConfiguration>(live1, backup1);
      
      bg.addConnectorPair(connectorPair);
      
      DiscoveryGroup dg = new DiscoveryGroupImpl(groupAddress, groupPort, timeout);
                  
      dg.start();
      
      bg.broadcastConnectors();
      
      boolean ok = dg.waitForBroadcast(1000);
      
      assertTrue(ok);
      
      List<Pair<TransportConfiguration, TransportConfiguration>> connectors = dg.getConnectors();
      
      assertNotNull(connectors);
      
      assertEquals(1, connectors.size());
      
      Pair<TransportConfiguration, TransportConfiguration> receivedPair = connectors.get(0);
      
      assertEquals(connectorPair, receivedPair);
      
      bg.stop();
      
      dg.stop();
      
   }
   
   public void testSimpleBroadcastDifferentAddress() throws Exception
   {      
      final InetAddress groupAddress = InetAddress.getByName("230.1.2.3");
      final int groupPort = 6745;
      final int timeout = 500;
      
      BroadcastGroup bg = new BroadcastGroupImpl(null, -1, groupAddress, groupPort);
      
      bg.start();
      
      TransportConfiguration live1 = generateTC();
      
      TransportConfiguration backup1 = generateTC();
      
      Pair<TransportConfiguration, TransportConfiguration> connectorPair = 
         new Pair<TransportConfiguration, TransportConfiguration>(live1, backup1);
      
      bg.addConnectorPair(connectorPair);
      
      final InetAddress groupAddress2 = InetAddress.getByName("230.1.2.4");
      
      DiscoveryGroup dg = new DiscoveryGroupImpl(groupAddress2, groupPort, timeout);
                  
      dg.start();
      
      bg.broadcastConnectors();
      
      boolean ok = dg.waitForBroadcast(1000);
      
      assertFalse(ok);
      
      bg.stop();
      
      dg.stop();      
   }
   
   public void testSimpleBroadcastDifferentPort() throws Exception
   {      
      final InetAddress groupAddress = InetAddress.getByName("230.1.2.3");
      final int groupPort = 6745;
      final int timeout = 500;
      
      BroadcastGroup bg = new BroadcastGroupImpl(null, -1, groupAddress, groupPort);
      
      bg.start();
      
      TransportConfiguration live1 = generateTC();
      
      TransportConfiguration backup1 = generateTC();
      
      Pair<TransportConfiguration, TransportConfiguration> connectorPair = 
         new Pair<TransportConfiguration, TransportConfiguration>(live1, backup1);
      
      bg.addConnectorPair(connectorPair);
           
      final int port2 = 6746;
      
      DiscoveryGroup dg = new DiscoveryGroupImpl(groupAddress, port2, timeout);
                  
      dg.start();
      
      bg.broadcastConnectors();
      
      boolean ok = dg.waitForBroadcast(1000);
      
      assertFalse(ok);
      
      bg.stop();
      
      dg.stop();      
   }
   
   public void testSimpleBroadcastDifferentAddressAndPort() throws Exception
   {      
      final InetAddress groupAddress = InetAddress.getByName("230.1.2.3");
      final int groupPort = 6745;
      final int timeout = 500;
      
      BroadcastGroup bg = new BroadcastGroupImpl(null, -1, groupAddress, groupPort);
      
      bg.start();
      
      TransportConfiguration live1 = generateTC();
      
      TransportConfiguration backup1 = generateTC();
      
      Pair<TransportConfiguration, TransportConfiguration> connectorPair = 
         new Pair<TransportConfiguration, TransportConfiguration>(live1, backup1);
      
      bg.addConnectorPair(connectorPair);
           
      final InetAddress groupAddress2 = InetAddress.getByName("230.1.2.4");
      final int port2 = 6746;
      
      DiscoveryGroup dg = new DiscoveryGroupImpl(groupAddress2, port2, timeout);
                  
      dg.start();
      
      bg.broadcastConnectors();
      
      boolean ok = dg.waitForBroadcast(1000);
      
      assertFalse(ok);
      
      bg.stop();
      
      dg.stop();      
   }
   
   private TransportConfiguration generateTC()
   {
      String className = "org.foo.bar." + UUIDGenerator.getInstance().generateStringUUID();
      String name = UUIDGenerator.getInstance().generateStringUUID();
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(UUIDGenerator.getInstance().generateStringUUID(), 123);
      params.put(UUIDGenerator.getInstance().generateStringUUID(), UUIDGenerator.getInstance().generateStringUUID());
      params.put(UUIDGenerator.getInstance().generateStringUUID(), 721633.123d);
      TransportConfiguration tc = new TransportConfiguration(className, params, name);
      return tc;
   }

}
