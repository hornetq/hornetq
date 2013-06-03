/*
* JBoss, Home of Professional Open Source.
* Copyright 2010, Red Hat, Inc., and individual contributors
* as indicated by the @author tags. See the copyright.txt file in the
* distribution for a full listing of individual contributors.
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
package org.hornetq.tests.integration.cluster.failover;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSession.QueueQuery;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.tests.integration.cluster.util.TestableServer;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         8/1/12
 */
public class ReplicatedMultipleServerFailoverTest extends MultipleServerFailoverTestBase
{
   @Test
   public void testStartLiveFirst() throws Exception
   {
      for (TestableServer liveServer : liveServers)
      {
         liveServer.start();
      }
      for (TestableServer backupServer : backupServers)
      {
         backupServer.start();
      }
      waitForTopology(liveServers.get(0).getServer(), liveServers.size(), backupServers.size());
      sendCrashReceive();
   }

   @Test
   public void testStartBackupFirst() throws Exception
   {
      for (TestableServer backupServer : backupServers)
      {
         backupServer.start();
      }
      for (TestableServer liveServer : liveServers)
      {
         liveServer.start();
      }
      waitForTopology(liveServers.get(0).getServer(), liveServers.size(), liveServers.size());
      sendCrashReceive();
   }

   protected void sendCrashReceive() throws Exception
   {
      ServerLocator[] locators = new ServerLocator[liveServers.size()];
      try
      {
         for (int i = 0; i < locators.length; i++)
         {
            locators[i] = getServerLocator(i);
         }

         ClientSessionFactory[] factories = new ClientSessionFactory[liveServers.size()];
         for(int i = 0; i < factories.length; i++)
         {
            factories[i] = createSessionFactory(locators[i]);
         }

         ClientSession[] sessions = new ClientSession[liveServers.size()];
         for(int i = 0; i < factories.length; i++)
         {
            sessions[i] = createSession(factories[i], true, true);
            sessions[i].createQueue(MultipleServerFailoverTestBase.ADDRESS, MultipleServerFailoverTestBase.ADDRESS, null, true);
         }

         //make sure bindings are ready before sending messages
         for (int i = 0; i < liveServers.size(); i++)
         {
            this.waitForBindings(liveServers.get(i).getServer(), ADDRESS.toString(), true, 1, 0, 2000);
            this.waitForBindings(liveServers.get(i).getServer(), ADDRESS.toString(), false, 1, 0, 2000);
         }

         ClientProducer producer = sessions[0].createProducer(MultipleServerFailoverTestBase.ADDRESS);

         for (int i = 0; i < liveServers.size() * 100; i++)
         {
            ClientMessage message = sessions[0].createMessage(true);

            setBody(i, message);

            message.putIntProperty("counter", i);

            producer.send(message);
         }

         producer.close();

         for (TestableServer liveServer : liveServers)
         {
            waitForDistribution(MultipleServerFailoverTestBase.ADDRESS, liveServer.getServer(), 100);
         }


         for (TestableServer liveServer : liveServers)
         {
            liveServer.crash();
         }
         ClientConsumer[] consumers = new ClientConsumer[liveServers.size()];
         for(int i = 0; i < factories.length; i++)
         {
            consumers[i] = sessions[i].createConsumer(MultipleServerFailoverTestBase.ADDRESS);
            sessions[i].start();
         }

         for (int i = 0; i < 100; i++)
         {
            for (ClientConsumer consumer : consumers)
            {
               ClientMessage message = consumer.receive(1000);
               Assert.assertNotNull("expecting durable msg " + i, message);
               message.acknowledge();
            }

         }
      }
      finally
      {
         for (ServerLocator locator : locators)
         {
            if(locator != null)
            {
               try
               {
                  locator.close();
               }
               catch (Exception e)
               {
                  //ignore
               }
            }
         }
      }
   }

   @Override
   public int getLiveServerCount()
   {
      return 2;
   }

   @Override
   public int getBackupServerCount()
   {
      return 2;
   }

   @Override
   public boolean useNetty()
   {
      return false;
   }

   @Override
   public boolean isSharedStore()
   {
      return false;
   }

   @Override
   public String getNodeGroupName()
   {
      return "nodeGroup";
   }

   @Override
   /*
   * for this test the 2 live connect to each other
   * */
   public void createLiveClusterConfiguration(int server, Configuration configuration, int servers)
   {
      TransportConfiguration livetc = getConnectorTransportConfiguration(true, server);
      configuration.getConnectorConfigurations().put(livetc.getName(), livetc);
      List<String> connectors = new ArrayList<String>();
      for(int i = 0; i < servers; i++)
      {
         if (i != server)
         {
            TransportConfiguration staticTc = getConnectorTransportConfiguration(true, i);
            configuration.getConnectorConfigurations().put(staticTc.getName(), staticTc);
            connectors.add(staticTc.getName());
         }
      }
      basicClusterConnectionConfig(configuration, livetc.getName(), connectors);

   }

   @Override
   /*
   * for this test the backups will connect to their own live server
   * */
   public void createBackupClusterConfiguration(int server, Configuration configuration, int servers)
   {
      TransportConfiguration backuptc = getConnectorTransportConfiguration(false, server);
      configuration.getConnectorConfigurations().put(backuptc.getName(), backuptc);
      List<String> connectors = new ArrayList<String>();
      for(int i = 0; i < servers; i++)
      {
         TransportConfiguration staticTc = getConnectorTransportConfiguration(true, i);
         configuration.getConnectorConfigurations().put(staticTc.getName(), staticTc);
         connectors.add(staticTc.getName());
      }
      for(int i = 0; i < servers; i++)
      {
         if (i != server)
         {
            TransportConfiguration staticTc = getConnectorTransportConfiguration(false, i);
            configuration.getConnectorConfigurations().put(staticTc.getName(), staticTc);
            connectors.add(staticTc.getName());
         }
      }
      basicClusterConnectionConfig(configuration, backuptc.getName(), connectors);
   }
}
