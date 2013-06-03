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

import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.tests.integration.cluster.util.TestableServer;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ReplicatedMultipleServerFailoverExtraBackupsTest extends ReplicatedMultipleServerFailoverTest
{
   @Override
   @Test
   public void testStartLiveFirst() throws Exception
   {
      backupServers.get(2).getServer().getConfiguration().setBackupGroupName(getNodeGroupName() + "-0");
      backupServers.get(3).getServer().getConfiguration().setBackupGroupName(getNodeGroupName() + "-1");

      startServers(liveServers);
      startServers(backupServers);
      waitForBackups();

      sendCrashReceive();
      waitForTopology(backupServers.get(0).getServer(), liveServers.size(), 2);
      sendCrashBackupReceive();
   }

   private void waitForBackups() throws InterruptedException
   {
      for (TestableServer backupServer : backupServers)
      {
         waitForComponent(backupServer.getServer(), 5);
      }
   }

   private void startServers(List<TestableServer> servers) throws Exception
   {
      for (TestableServer testableServer : servers)
      {
         testableServer.start();
      }
   }

   @Override
   @Test
   public void testStartBackupFirst() throws Exception
   {
      backupServers.get(2).getServer().getConfiguration().setBackupGroupName(getNodeGroupName() + "-0");
      backupServers.get(3).getServer().getConfiguration().setBackupGroupName(getNodeGroupName() + "-1");

      startServers(backupServers);
      startServers(liveServers);
      waitForBackups();

      waitForTopology(liveServers.get(0).getServer(), liveServers.size(), 2);
      sendCrashReceive();
   }

   protected void sendCrashBackupReceive() throws Exception
   {
      ServerLocator locator0 = getBackupServerLocator(0);
      ServerLocator locator1 = getBackupServerLocator(1);

      ClientSessionFactory factory0 = createSessionFactory(locator0);
      ClientSessionFactory factory1 = createSessionFactory(locator1) ;

      ClientSession session0 = factory0.createSession(false, true, true);
      ClientSession session1 = factory1.createSession(false, true, true);

      ClientProducer producer = session0.createProducer(MultipleServerFailoverTestBase.ADDRESS);

      for (int i = 0; i < 200; i++)
      {
         ClientMessage message = session0.createMessage(true);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      producer.close();

      waitForDistribution(MultipleServerFailoverTestBase.ADDRESS, backupServers.get(0).getServer(), 100);
      waitForDistribution(MultipleServerFailoverTestBase.ADDRESS, backupServers.get(1).getServer(), 100);

      List<TestableServer> toCrash = new ArrayList<TestableServer>();
      for (TestableServer backupServer : backupServers)
      {
         if(!backupServer.getServer().getConfiguration().isBackup())
         {
            toCrash.add(backupServer);
         }
      }

      for (TestableServer testableServer : toCrash)
      {
         testableServer.crash();
      }

      ClientConsumer consumer0 = session0.createConsumer(MultipleServerFailoverTestBase.ADDRESS);
      ClientConsumer consumer1 = session1.createConsumer(MultipleServerFailoverTestBase.ADDRESS);
      session0.start();
      session1.start();


      for (int i = 0; i < 100; i++)
      {
         ClientMessage message = consumer0.receive(1000);
         Assert.assertNotNull("expecting durable msg " + i, message);
         message.acknowledge();
         consumer1.receive(1000);
         Assert.assertNotNull("expecting durable msg " + i, message);
         message.acknowledge();

      }
   }


   @Override
   public int getBackupServerCount()
   {
      return 4;
   }
}
