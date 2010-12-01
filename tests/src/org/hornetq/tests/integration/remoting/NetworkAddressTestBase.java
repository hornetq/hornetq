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

package org.hornetq.tests.integration.remoting;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A NetworkAddressTest
 *
 * @author jmesnil
 * 
 * Created 26 janv. 2009 15:06:58
 *
 *
 */
public abstract class NetworkAddressTestBase extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   static
   {
      try
      {
         Map<NetworkInterface, InetAddress> map = NetworkAddressTestBase.getAddressForEachNetworkInterface();
         StringBuilder s = new StringBuilder("using network settings:\n");
         Set<Entry<NetworkInterface, InetAddress>> set = map.entrySet();
         for (Entry<NetworkInterface, InetAddress> entry : set)
         {
            s.append(entry.getKey().getDisplayName() + ": " + entry.getValue().getHostAddress() + "\n");
         }
         System.out.println(s);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }

   }

   public static Map<NetworkInterface, InetAddress> getAddressForEachNetworkInterface() throws Exception
   {
      Map<NetworkInterface, InetAddress> map = new HashMap<NetworkInterface, InetAddress>();
      Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
      while (ifaces.hasMoreElements())
      {
         NetworkInterface iface = ifaces.nextElement();
         Enumeration<InetAddress> enumeration = iface.getInetAddresses();
         while (enumeration.hasMoreElements())
         {
            InetAddress inetAddress = enumeration.nextElement();
            if (inetAddress instanceof Inet4Address)
            {
               map.put(iface, inetAddress);
               break;
            }
         }
      }

      return map;
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testConnectToServerWithSameHost() throws Exception
   {
      Map<NetworkInterface, InetAddress> map = NetworkAddressTestBase.getAddressForEachNetworkInterface();
      if (map.size() > 0)
      {
         Set<Entry<NetworkInterface, InetAddress>> set = map.entrySet();
         Iterator<Entry<NetworkInterface, InetAddress>> iterator = set.iterator();
         InetAddress address = iterator.next().getValue();
         String host = address.getHostAddress();
         testConnection(host, host, true);
      }
   }

   public void testConnectToServerAcceptingAllHosts() throws Exception
   {
      Map<NetworkInterface, InetAddress> map = NetworkAddressTestBase.getAddressForEachNetworkInterface();
      Set<Entry<NetworkInterface, InetAddress>> set = map.entrySet();
      for (Entry<NetworkInterface, InetAddress> entry : set)
      {
         String host = entry.getValue().getHostAddress();
         testConnection("0.0.0.0", host, true);
      }
   }

   public void testConnectToServerAcceptingOnlyAnotherHost() throws Exception
   {
      Map<NetworkInterface, InetAddress> map = NetworkAddressTestBase.getAddressForEachNetworkInterface();
      if (map.size() <= 1)
      {
         System.err.println("There must be at least 1 network interfaces: test will not be executed");
         return;
      }

      Set<Entry<NetworkInterface, InetAddress>> set = map.entrySet();
      Iterator<Entry<NetworkInterface, InetAddress>> iterator = set.iterator();
      Entry<NetworkInterface, InetAddress> acceptorEntry = iterator.next();
      Entry<NetworkInterface, InetAddress> connectorEntry = iterator.next();

      testConnection(acceptorEntry.getValue().getHostName(), connectorEntry.getValue().getHostAddress(), false);
   }

   public void testConnectorToServerAcceptingAListOfHosts() throws Exception
   {
      Map<NetworkInterface, InetAddress> map = NetworkAddressTestBase.getAddressForEachNetworkInterface();
      if (map.size() <= 1)
      {
         System.err.println("There must be at least 2 network interfaces: test will not be executed");
         return;
      }

      Set<Entry<NetworkInterface, InetAddress>> set = map.entrySet();
      Iterator<Entry<NetworkInterface, InetAddress>> iterator = set.iterator();
      Entry<NetworkInterface, InetAddress> entry1 = iterator.next();
      Entry<NetworkInterface, InetAddress> entry2 = iterator.next();

      String listOfHosts = entry1.getValue().getHostAddress() + ", " + entry2.getValue().getHostAddress();

      testConnection(listOfHosts, entry1.getValue().getHostAddress(), true);
      testConnection(listOfHosts, entry2.getValue().getHostAddress(), true);
   }

   public void testConnectorToServerAcceptingAListOfHosts_2() throws Exception
   {
      Map<NetworkInterface, InetAddress> map = NetworkAddressTestBase.getAddressForEachNetworkInterface();
      if (map.size() <= 2)
      {
         System.err.println("There must be at least 3 network interfaces: test will not be executed");
         return;
      }

      Set<Entry<NetworkInterface, InetAddress>> set = map.entrySet();
      Iterator<Entry<NetworkInterface, InetAddress>> iterator = set.iterator();
      Entry<NetworkInterface, InetAddress> entry1 = iterator.next();
      Entry<NetworkInterface, InetAddress> entry2 = iterator.next();
      Entry<NetworkInterface, InetAddress> entry3 = iterator.next();

      String listOfHosts = entry1.getValue().getHostAddress() + ", " + entry2.getValue().getHostAddress();

      testConnection(listOfHosts, entry1.getValue().getHostAddress(), true);
      testConnection(listOfHosts, entry2.getValue().getHostAddress(), true);
      testConnection(listOfHosts, entry3.getValue().getHostAddress(), false);
   }

   public void testConnection(final String acceptorHost, final String connectorHost, final boolean mustConnect) throws Exception
   {
      System.out.println("acceptor=" + acceptorHost + ", connector=" + connectorHost + ", mustConnect=" + mustConnect);

      Map<String, Object> params = new HashMap<String, Object>();
      params.put(getHostPropertyKey(), acceptorHost);
      TransportConfiguration acceptorConfig = new TransportConfiguration(getAcceptorFactoryClassName(), params);
      Set<TransportConfiguration> transportConfigs = new HashSet<TransportConfiguration>();
      transportConfigs.add(acceptorConfig);

      Configuration config = createDefaultConfig(true);
      config.setAcceptorConfigurations(transportConfigs);
      HornetQServer messagingService = createServer(false, config);
      messagingService.start();

      params = new HashMap<String, Object>();
      params.put(getHostPropertyKey(), connectorHost);
      TransportConfiguration connectorConfig = new TransportConfiguration(getConnectorFactoryClassName(), params);

      try
      {
         ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(connectorConfig);


         if (mustConnect)
         {
            ClientSessionFactory sf = locator.createSessionFactory();
            sf.close();
            System.out.println("connection OK");
         }
         else
         {
            try
            {
               ClientSessionFactory sf = locator.createSessionFactory();
               Assert.fail("session creation must fail because connector must not be able to connect to the server bound to another network interface");
            }
            catch (Exception e)
            {
            }
         }
      }
      finally
      {
         if (messagingService != null)
         {
            messagingService.stop();
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected abstract String getAcceptorFactoryClassName();

   protected abstract String getConnectorFactoryClassName();

   protected abstract String getHostPropertyKey();

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
